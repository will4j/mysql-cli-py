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
        values = self.parse_sql_params(*args, **kwargs)
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
        values = self.parse_sql_params(*args, **kwargs)
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

class SelectManyByQueryClauses(Select):
    """Execute select sql by query clauses(es: where,limit,offer,groupby,orderby and so on) and return many rows.
       use ":+word" as placeholder, like ":name"

       notice:
        SELECT column1, column2, ...
        FROM table_name
        WHERE condition
        GROUP BY column1, column2, ...
        HAVING condition
        ORDER BY column1, column2, ...
        LIMIT start, count;

    """

    def execute_sql(self, cnx, cur, *args, **kwargs):
        placeholders = re.findall(r':\w+', self.sql)  # 统计sql语句的占位符
        params = kwargs['params']
        if len(params) < len(placeholders):  # 简单匹配下参数个数对不对
            return None  # 参数个数匹配不上就直接返回，TODO 加上报错
        values = []
        replacePh = []
        for tmp in placeholders:
            ph = tmp[1:]
            if ph in params.keys():
                if ph == "groupby" or ph == "orderby":   # groupby和orderby 不支持占位符，直接替换
                    # 加入校验，因为无法使用占位符，不校验有SQL注入风险，判断数据只能包含大小写字母，空格，数字和下划线
                    if not re.match(r'^[a-zA-Z0-9_ ]*$', params[ph]):
                        return None
                    if isinstance(params[ph], tuple):
                        replacePh.append(','.join(params[ph]))
                    elif isinstance(params[ph], list):
                        replacePh.append(','.join(params[ph]))
                    else:
                        replacePh.append(params[ph])
                else:
                    if isinstance(params[ph], tuple):
                        replacePh.append( ', '.join(['?'] * len(params[ph])))
                        values += list(params[ph])
                    elif isinstance(params[ph], list):
                        replacePh.append(', '.join(['?'] * len(params[ph])))
                        values += params[ph]
                    else:
                        replacePh.append('?')
                        values.append(params[ph])
            else:
                return None  # 有一个参数匹配不上就直接返回，TODO 加上报错
        values = tuple(values)
        for i in range(len(placeholders)):
            self.sql = self.sql.replace(placeholders[i],replacePh[i])
        cur.execute(self.sql, values)
        tuple_rows = cur.fetchall()
        if self.dictionary:
            return [_convert_tuple_row_to_dict(cur.column_names, row) for row in tuple_rows]
        else:
            return tuple_rows


class Update(_BaseQuery):
    """Execute update sql and return affected row numbers

    """

    def execute_sql(self, cnx, cur, *args, **kwargs):
        values = self.parse_sql_params(*args, **kwargs)
        cur.execute(self.sql, values)
        return cur.rowcount


class Delete(_BaseQuery):
    """Execute delete sql and return affected row numbers

    """

    def execute_sql(self, cnx, cur, *args, **kwargs):
        values = self.parse_sql_params(*args, **kwargs)
        cur.execute(self.sql, values)
        return cur.rowcount
