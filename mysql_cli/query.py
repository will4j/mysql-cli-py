import functools

from mysql.connector.cursor import MySQLCursorPrepared

import mysql_cli


def insert(sql, param_converter=None):
    """
    execute insert sql

    :param sql: insert sql
    :param param_converter: convert to param tuple
    :return: inserted record id
    """

    def decorator_insert(func):
        @functools.wraps(func)
        def wrapper_insert(*args, **kwargs):
            values = _parse_sql_params(param_converter, *args, **kwargs)

            with mysql_cli.get_connection() as cnx:
                with cnx.cursor(cursor_class=MySQLCursorPrepared) as cur:
                    cur.execute(sql, values)
                    return cur.lastrowid

        return wrapper_insert

    return decorator_insert


def select(sql, param_converter=None):
    def decorator_select(func):
        @functools.wraps(func)
        def wrapper_select(*args, **kwargs):
            values = _parse_sql_params(param_converter, *args, **kwargs)

            with mysql_cli.get_connection() as cnx:
                with cnx.cursor(cursor_class=MySQLCursorPrepared) as cur:
                    cur.execute(sql, values)
                    return cur.fetchone()

        return wrapper_select

    return decorator_select


def _parse_sql_params(param_converter, *args, **kwargs):
    if param_converter is not None:
        values = param_converter(*args, **kwargs)
    elif not isinstance(args, tuple):
        values = (args,)
    else:
        values = args
    return values
