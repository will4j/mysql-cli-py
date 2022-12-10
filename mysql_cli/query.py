import functools
from abc import abstractmethod

import mysql_cli


class _BaseQuery:
    def __init__(self, sql, param_converter=None):
        """Init base decorator.

        :param sql: sql statement to execute
        :param param_converter: param_converter
        """
        self.sql = sql
        self.param_converter = param_converter

    def parse_sql_params(self, *args, **kwargs):
        """Convert func param to sql param.

        1. call param_converter which expect to return params in tuple if exists
        2. if args not instance of tuple, make a single value tuple, like (1,)
        3. if args is tuple itself, then use it directly
        :param args: function call args
        :param kwargs: function call kwargs
        :return: params tuple
        """
        if self.param_converter is not None:
            values = self.param_converter(*args, **kwargs)
        elif not isinstance(args, tuple):
            values = (args,)
        else:
            values = args
        return values

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
        @functools.wraps(func)
        def wrapped(*args, **kwargs):
            with mysql_cli.get_connection() as cnx:
                with cnx.cursor(prepared=True) as cur:
                    return self.execute_sql(cnx, cur, *args, **kwargs)

        return wrapped


class Insert(_BaseQuery):

    def execute_sql(self, cnx, cur, *args, **kwargs):
        values = self.parse_sql_params(*args, **kwargs)
        cur.execute(self.sql, values)
        return cur.lastrowid


class Select(_BaseQuery):

    def execute_sql(self, cnx, cur, *args, **kwargs):
        values = self.parse_sql_params(*args, **kwargs)
        cur.execute(self.sql, values)
        tuple_row = cur.fetchone()
        # convert tuple to dict with column names
        return dict(zip(cur.column_names, tuple_row))
