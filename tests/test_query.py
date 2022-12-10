import os

import mysql_cli
from mysql_cli import insert, select

TESTS_PATH = os.path.dirname(__file__)


def setup():
    mysql_cli.init_from_conf_file(os.path.join(TESTS_PATH, "test_mysql.toml"))
    print("mysql inited")
    with mysql_cli.get_connection() as cnx:
        with cnx.cursor() as cur:
            stmt_drop = "DROP TABLE IF EXISTS my_test;"
            cur.execute(stmt_drop)

            stmt_create = (
                "CREATE TABLE my_test ("
                "id TINYINT UNSIGNED NOT NULL AUTO_INCREMENT, "
                "name VARCHAR(30) DEFAULT '' NOT NULL, "
                "cnt TINYINT UNSIGNED DEFAULT 0, "
                "PRIMARY KEY (id))"
            )
            cur.execute(stmt_create)


def _insert_param(param: dict):
    return param["name"], param["cnt"]


@insert("insert into my_test (name, cnt) values (%s, %s);", _insert_param)
def insert_one(param: dict):
    pass


@select("select id, name, cnt from my_test where name = %s limit 1;")
def select_one(name):
    pass


def test_insert_one():
    assert insert_one({"name": "hello", "cnt": 2}) == 1
    assert insert_one({"name": "hello", "cnt": 2}) == 2


def test_select_one():
    row = select_one("hello")
    assert row == (1, "hello", 2)
