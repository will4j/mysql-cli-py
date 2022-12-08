import os

import pytest

from mysql_cli.client import parse_mysql_conf
import mysql_cli

TESTS_PATH = os.path.dirname(__file__)


def test_not_init():
    with pytest.raises(RuntimeError) as e:
        mysql_cli.get_connection()
    exec_msg = e.value.args[0]
    assert exec_msg.startswith("MySQL not inited,")


def test_parse_mysql_conf():
    mysql_conf = parse_mysql_conf(os.path.join(TESTS_PATH, "test_parse_mysql.toml"))
    assert isinstance(mysql_conf, dict)
    assert mysql_conf["pool_name"] == "mypool"
    assert mysql_conf["pool_size"] == 6
    assert mysql_conf["host"] == "127.0.0.1"
    assert mysql_conf["port"] == 3306
    assert mysql_conf["db"] == "mydb"
    assert mysql_conf["user"] == "mydb_user"
    assert mysql_conf["password"] == "mydb_password"
    assert mysql_conf["charset"] == "utf8mb4"
    assert mysql_conf["pool_reset_session"]
