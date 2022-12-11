import os

import mysql_cli
from mysql_cli import BatchInsert, Delete, Insert, Select, SelectMany, Transactional, Update
from mysql_cli.query import thread_local

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


@Insert("insert into my_test (name, cnt) values (?, ?);")
def insert_with_dict(param: dict):
    return param["name"], param["cnt"]


@Insert("insert into my_test (name, cnt) values (?, ?);")
def insert_with_param(name, cnt):
    return name, cnt


@BatchInsert("insert into my_test (name, cnt) values (?, ?);")
def batch_insert(params):
    return tuple((row["name"], row["cnt"]) for row in params)


@Select("select id, name, cnt from my_test where name = ? limit 1;", dictionary=False)
def select_one_return_tuple(name):
    return name


@Select("select id, name, cnt from my_test where name = ? limit 1;")
def select_one_return_dict(name):
    return name


@SelectMany("select name, cnt from my_test where name = ? and cnt >= ? order by cnt desc;")
def select_many_by_name(name, cnt):
    return name, cnt


@Update("update my_test set cnt = ? where name = ? limit ?;")
def update_cnt_by_name(name, cnt, limit=10):
    return cnt, name, limit


@Delete("delete from my_test where name = ? limit ?;")
def delete_by_name(name, limit=10):
    return name, limit


def test_batch_insert():
    params = [{"name": "world", "cnt": 1}, {"name": "world", "cnt": 2}, {"name": "world", "cnt": 3}]
    assert batch_insert(params) == 3


def test_select_one():
    row = select_one_return_tuple("hello")
    assert row == (1, 'hello', 2)
    row = select_one_return_dict("hello")
    assert row == {'id': 1, 'name': 'hello', 'cnt': 2}


def test_select_many():
    rows = select_many_by_name("hello", 1)
    assert len(rows) == 2
    assert rows[0]["cnt"] == 3
    assert rows[1]["name"] == "hello"
    assert "id" not in rows[0]


def test_update_one():
    insert_with_param("update_one", 1)
    insert_with_param("update_one", 2)
    insert_with_param("update_one", 3)

    assert update_cnt_by_name("update_one", 0, 1) == 1
    assert len(select_many_by_name("update_one", 1)) == 2


def test_update_many():
    insert_with_param("update_many", 1)
    insert_with_param("update_many", 2)
    insert_with_param("update_many", 3)

    assert update_cnt_by_name("update_many", 0) == 3
    assert select_many_by_name("update_many", 1) == []


def test_delete_one():
    insert_with_param("delete_one", 1)
    insert_with_param("delete_one", 2)

    assert delete_by_name("delete_one", 1) == 1
    assert select_one_return_dict("delete_one")["cnt"] == 2


def test_delete_many():
    insert_with_param("delete_many", 1)
    insert_with_param("delete_many", 2)

    assert delete_by_name("delete_many") == 2
    assert select_one_return_dict("delete_many") is None


@Transactional
def transaction_rollback():
    params = [{"name": "tx_rollback", "cnt": 1}, {"name": "tx_rollback", "cnt": 2}]
    batch_insert(params)

    assert select_one_return_dict("tx_rollback")["name"] == "tx_rollback"
    update_cnt_by_name("tx_rollback", 3)
    assert "tx_cnx" in thread_local.__dict__
    raise RuntimeError("rollback")


def test_transaction_rollback():
    try:
        transaction_rollback()
    except RuntimeError:
        pass
    assert "tx_cnx" not in thread_local.__dict__
    assert select_one_return_dict("with_transaction") is None


@Transactional
def transaction_commit():
    params = [{"name": "tx_commit", "cnt": 1}, {"name": "tx_commit", "cnt": 2}]
    batch_insert(params)
    update_cnt_by_name("tx_commit", 3)
    assert "tx_cnx" in thread_local.__dict__


def test_transaction_commit():
    transaction_commit()
    assert "tx_cnx" not in thread_local.__dict__
    assert select_one_return_dict("tx_commit")["name"] == "tx_commit"
    assert select_one_return_dict("tx_commit")["cnt"] == 3
