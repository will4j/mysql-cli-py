from mysql.connector.pooling import PooledMySQLConnection

from mysql_cli.client import MySQLWrapper


def init_from_conf_dict(mysql_conf: dict):
    return MySQLWrapper.init_from_conf_dict(mysql_conf)


def init_from_conf_file(mysql_conf_file):
    return MySQLWrapper.init_from_conf_file(mysql_conf_file)


def get_connection() -> PooledMySQLConnection:
    return MySQLWrapper.get_connection()
