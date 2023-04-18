import functools
import threading
import types
from abc import abstractmethod
import re

import mysql_cli

thread_local = threading.local()


class Transactional:
    def __init__(self, func):
        functools.update_wrapper(self, func)
        self.func = func

    def __call__(self, *args, **kwargs):
        return self.execute_in_wrapper(*args, **kwargs)

    def __get__(self, instance, cls):
        if instance is None:
            return self
        else:
            return types.MethodType(self, instance)

    def execute_in_wrapper(self, *args, **kwargs):
        tx_cnx, _ = _get_tx_cnx_and_cur(create=True)
        if not tx_cnx.in_transaction:
            # new transaction on top level, control commit and rollback
            try:
                tx_cnx.start_transaction()
                result = self.func(*args, **kwargs)
                tx_cnx.commit()
                return result
            except:
                tx_cnx.rollback()
                raise
            finally:
                # clear thread local connection status
                _clear_tx_status()
        else:
            # already in transaction
            return self.func(*args, **kwargs)


def _get_tx_cnx_and_cur(create=True):
    tx_cnx = thread_local.__dict__.get("tx_cnx")
    if not tx_cnx and create:
        tx_cnx = mysql_cli.get_connection()
        thread_local.tx_cnx = tx_cnx

    tx_cur = thread_local.__dict__.get("tx_cur")
    if not tx_cur and create:
        tx_cur = tx_cnx.cursor(prepared=True)
        thread_local.tx_cur = tx_cur

    return tx_cnx, tx_cur


def _clear_tx_status():
    tx_cur = thread_local.__dict__.get("tx_cur")
    if tx_cur:
        tx_cur.close()
        del thread_local.tx_cur

    tx_cnx = thread_local.__dict__.get("tx_cnx")
    if tx_cnx:
        tx_cnx.close()
        del thread_local.tx_cnx


class _BaseQuery:
    def __init__(self, sql):
        """Init base decorator.

        :param sql: sql statement to execute
        """
        self.sql = sql

    @abstractmethod
    def execute_sql(self, cnx, cur, *args, **kwargs):
        """Implement how to deal with sql.

        After execution, the connection and cursor with be closed automatically
        :param cnx: mysql connection object
        :param cur: prepared cursor created from cnx
        :param args: function call args
        :param kwargs: function call kwargs
        :return:
        """
        raise NotImplementedError("implement execute_sql method in subclass.")

    def __call__(self, func):
        self.func = func

        @functools.wraps(func)
        def wrapped(*args, **kwargs):
            return self.execute_in_wrapper(*args, **kwargs)

        return wrapped

    def execute_in_wrapper(self, *args, **kwargs):
        tx_cnx, tx_cur = _get_tx_cnx_and_cur(create=False)
        if tx_cnx and tx_cur:
            # use shared thread local connection
            return self.execute_sql(tx_cnx, tx_cur, *args, **kwargs)
        else:
            with mysql_cli.get_connection() as cnx:
                with cnx.cursor(prepared=True) as cur:
                    return self.execute_sql(cnx, cur, *args, **kwargs)

    def parse_sql_params(self, *args, **kwargs):
        """Convert func param to sql param.

        1. try use wrapped func 's return value if not None, else use args
        2. if args not instance of tuple, make a single value tuple, like (1,)
        3. if args is tuple itself, then use it directly
        :param args: function call args
        :param kwargs: function call kwargs
        :return: params tuple
        """

        def make_tuple(param):
            if not isinstance(param, tuple):
                return param,
            else:
                return param

        returned_param = self.func(*args, **kwargs)
        if returned_param is not None:
            values = make_tuple(returned_param)
        else:
            values = make_tuple(args)
        return values

    def parse_search_and_update_sql_params(self, *args, **kwargs):
        """ support use ":word" or ? as a placeholder, but when the use of ":word" placeholder does not allow to use "?" as a placeholder

        1. check which type of placeholder be used
        2. if using "?" as a placeholder, expand the amount of "?" based on the length of each parameter
        3. if using ":word" as a placeholder, replace ":word" with a "?", expand the amout of "?" according to
           the number of values for "word" in the input parameter
        :param args: function call args
        :param kwargs: function call kwargs
        :return: params tuple
        """

        def handle_sql_values_by_question_placeholders(params):  # 处理问号占位符
            values = []
            new_sql = ''
            placeholder_count = self.sql.count('?')
            tmp = self.sql.split('?')
            if len(params) != placeholder_count or placeholder_count == 0:
                return ()  # TODO 现在只是返回空内容，后续优化，加入报错
            # 修改sql语句将每个占位符?按实参的个数扩展，并将实参拼成一个tuple
            for i in range(placeholder_count):
                new_sql += tmp[i]
                if isinstance(params[i], tuple):
                    values += list(params[i])
                    new_sql += ', '.join(['?'] * len(params[i]))
                elif isinstance(params[i], list):
                    values += params[i]
                    new_sql += ', '.join(['?'] * len(params[i]))
                else:
                    values.append(params[i])
                    new_sql += '?'
            new_sql += tmp[placeholder_count]
            self.sql = new_sql
            return tuple(values)


        def handle_sql_values_by_word_placeholders(params, placeholders):  # 处理问号占位符
            values = []
            if len(params) != len(placeholders):  # 简单匹配下参数个数对不对
                return ()  # 参数个数匹配不上就直接返回，TODO 加上报错
            replace_ph = []
            for tmp in placeholders:
                ph = tmp[1:]
                if ph in params.keys():
                    if ph == "groupby" or ph == "orderby":  # groupby和orderby 不支持占位符，直接替换
                        # 加入校验，因为无法使用占位符，不校验有SQL注入风险，判断数据只能包含大小写字母，空格，数字和下划线
                        if not re.match(r'^[a-zA-Z0-9_ ]*$', params[ph]):
                            return None
                        if isinstance(params[ph], tuple):
                            replace_ph.append(','.join(params[ph]))
                        elif isinstance(params[ph], list):
                            replace_ph.append(','.join(params[ph]))
                        else:
                            replace_ph.append(params[ph])
                    else:
                        if isinstance(params[ph], tuple):
                            replace_ph.append(', '.join(['?'] * len(params[ph])))
                            values += list(params[ph])
                        elif isinstance(params[ph], list):
                            replace_ph.append(', '.join(['?'] * len(params[ph])))
                            values += params[ph]
                        else:
                            replace_ph.append('?')
                            values.append(params[ph])
                else:
                    return ()  # 有一个参数匹配不上就直接返回，TODO 加上报错
            values = tuple(values)
            for i in range(len(placeholders)):
                self.sql = self.sql.replace(placeholders[i], replace_ph[i])
            return values

        return_params = self.func(*args, **kwargs)
        placeholders_list = re.findall(r':\w+', self.sql)  # 统计sql语句的占位符

        if len(placeholders_list) == 0:  # 使用问号做占位符
            if return_params is None:
                return_params = args
            if not isinstance(return_params, tuple):
                return_params = return_params,  # 如果不是元组，则转化成元组
            return handle_sql_values_by_question_placeholders(return_params)
        else:  # 使用":+word"的形式作为占位符
            if '?' in self.sql:
                raise ValueError('the use of ":word" placeholder does not allow to use "?" as a placeholder')
            if return_params is None:
                return_params = kwargs['params']
            if not isinstance(return_params, dict):
                return ()  # TODO 直接返回，后续可以加上报错
            return handle_sql_values_by_word_placeholders(return_params, placeholders_list)


class Insert(_BaseQuery):
    """Execute insert sql with one row and return autoincrement id

    """

    def execute_sql(self, cnx, cur, *args, **kwargs):
        values = self.parse_sql_params(*args, **kwargs)
        cur.execute(self.sql, values)
        return cur.lastrowid


class BatchInsert(Insert):
    """Execute insert sql with many row and return affected row numbers

    """

    @Transactional
    def execute_in_wrapper(self, *args, **kwargs):
        return super().execute_in_wrapper(*args, **kwargs)

    def execute_sql(self, cnx, cur, *args, **kwargs):
        values = self.parse_sql_params(*args, **kwargs)
        cur.executemany(self.sql, values)
        # https://dev.mysql.com/doc/connector-python/en/connector-python-api-mysqlcursor-rowcount.html
        return cur.rowcount


class Select(_BaseQuery):
    """Execute select sql and return one row.

    """

    def __init__(self, sql, dictionary=True):
        """Init base decorator.
        :param sql: sql statement to execute
        :param dictionary: rows are returned as dictionary instead of tuple
        """
        super().__init__(sql)
        self.dictionary = dictionary

    def execute_sql(self, cnx, cur, *args, **kwargs):
        values = self.parse_search_and_update_sql_params(*args, **kwargs)
        cur.execute(self.sql, values)
        tuple_row = cur.fetchone()
        if self.dictionary:
            return _convert_tuple_row_to_dict(cur.column_names, tuple_row)
        else:
            return tuple_row


class SelectMany(Select):
    """Execute select sql and return many rows.

    """

    def execute_sql(self, cnx, cur, *args, **kwargs):
        values = self.parse_search_and_update_sql_params(*args, **kwargs)
        cur.execute(self.sql, values)
        tuple_rows = cur.fetchall()
        if self.dictionary:
            return [_convert_tuple_row_to_dict(cur.column_names, row) for row in tuple_rows]
        else:
            return tuple_rows


def _convert_tuple_row_to_dict(column_names, tuple_row):
    # convert tuple to dict with column names
    # https://dev.mysql.com/doc/connector-python/en/connector-python-api-mysqlcursor-column-names.html
    if tuple_row:
        return dict(zip(column_names, tuple_row))


class Update(_BaseQuery):
    """Execute update sql and return affected row numbers

    """

    def execute_sql(self, cnx, cur, *args, **kwargs):
        values = self.parse_search_and_update_sql_params(*args, **kwargs)
        cur.execute(self.sql, values)
        return cur.rowcount


class Delete(_BaseQuery):
    """Execute delete sql and return affected row numbers

    """

    def execute_sql(self, cnx, cur, *args, **kwargs):
        values = self.parse_search_and_update_sql_params(*args, **kwargs)
        cur.execute(self.sql, values)
        return cur.rowcount
