import os

import mysql_cli
from mysql_cli import BatchInsert, Insert, Select, SelectMany, Update

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

            assert insert_with_dict({"name": "hello", "cnt": 2}) == 1
            assert insert_with_param("hello", 3) == 2


def _insert_param(param: dict):
    return param["name"], param["cnt"]


def _batch_insert_param(params):
    return (_insert_param(row) for row in params)


@Insert("insert into my_test (name, cnt) values (%s, %s);", _insert_param)
def insert_with_dict(param: dict):
    pass


@Insert("insert into my_test (name, cnt) values (%s, %s);")
def insert_with_param(name, cnt):
    pass


@BatchInsert("insert into my_test (name, cnt) values (%s, %s);", _batch_insert_param)
def batch_insert(params):
    pass


@Select("select id, name, cnt from my_test where name = %s limit 1;", dictionary=False)
def select_one_return_tuple(name):
    pass


@Select("select id, name, cnt from my_test where name = %s limit 1;")
def select_one_return_dict(name):
    pass


@SelectMany("select name, cnt from my_test where name = %s and cnt >= %s order by cnt desc;")
def select_many(name, cnt):
    pass


@Update("update my_test set cnt = %s where name = %s;")
def update(cnt, name):
    pass


def test_batch_insert():
    params = [{"name": "world", "cnt": 1}, {"name": "world", "cnt": 2}, {"name": "world", "cnt": 3}]
    # todo why -3 ?
    assert batch_insert(params) == -3


def test_select_one():
    row = select_one_return_tuple("hello")
    assert row == (1, 'hello', 2)
    row = select_one_return_dict("hello")
    assert row == {'id': 1, 'name': 'hello', 'cnt': 2}


def test_select_many():
    rows = select_many("hello", 1)
    assert len(rows) == 2
    assert rows[0]["cnt"] == 3
    assert rows[1]["name"] == "hello"
    assert "id" not in rows[0]


def test_update():
    assert update(1, "hello") == 2
    assert select_one_return_dict("hello")["cnt"] == 1
