import functools
from abc import abstractmethod

import mysql.connector

import mysql_cli


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
            with mysql_cli.get_connection() as cnx:
                with cnx.cursor(prepared=True) as cur:
                    return self.execute_sql(cnx, cur, *args, **kwargs)

        return wrapped

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


def _convert_tuple_row_to_dict(column_names, tuple_row):
    # convert tuple to dict with column names
    # https://dev.mysql.com/doc/connector-python/en/connector-python-api-mysqlcursor-column-names.html
    if tuple_row:
        return dict(zip(column_names, tuple_row))


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

    def execute_sql(self, cnx, cur, *args, **kwargs):
        values = self.parse_sql_params(*args, **kwargs)
        try:
            cnx.start_transaction()
            cur.executemany(self.sql, values)
            cnx.commit()
        except mysql.connector.Error as e:
            cnx.rollback()

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
