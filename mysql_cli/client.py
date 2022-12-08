import tomli

from mysql.connector.pooling import MySQLConnectionPool, PooledMySQLConnection


class MySQLWrapper:
    # borg pattern
    _shared_state = {}

    def __init__(self, pool: MySQLConnectionPool = None):
        self.__dict__ = self._shared_state
        if pool is not None:
            self.mysql_connection_pool = pool

    @classmethod
    def get_connection(cls) -> PooledMySQLConnection:
        new_instance = cls()
        if not hasattr(new_instance, "mysql_connection_pool"):
            raise RuntimeError("MySQL not inited, see mysql_cli.init_from_* method.")
        return new_instance.mysql_connection_pool.get_connection()

    @classmethod
    def init_from_conf_dict(cls, mysql_conf: dict):
        return cls(MySQLConnectionPool(**mysql_conf))

    @classmethod
    def init_from_conf_file(cls, mysql_conf_file):
        """
        mysql conf from toml file
        :param mysql_conf_file: toml file path
        :return:
        """
        mysql_conf = parse_mysql_conf(mysql_conf_file)
        return cls(MySQLConnectionPool(**mysql_conf))


def parse_mysql_conf(mysql_conf_file):
    with open(mysql_conf_file) as f:
        return tomli.loads(f.read())
