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

@Select("select id, name, cnt from my_test where name = 'hello' limit 1 offset 1;", dictionary=False)
def select_one_return_tuple_full():
    pass



@Select("select id, name, cnt from my_test where name = ? and cnt in (?) limit 1;", dictionary=False)
def select_one_by_in(name,cnt):
    return name,cnt

@Select("select id, name, cnt from my_test where name = ? and cnt in (?) limit ? offset ?;", dictionary=False)
def select_one_by_in_more_condition(name,cnt,limit,offset):
    return name,cnt,limit,offset

@Select("select id, name, cnt from my_test where name = :name and cnt in (:cnt) limit :limit offset :offset;", dictionary=False)
def select_one_by_word_placeholders(params: dict):
    return params


@Select("select id, name, cnt from my_test where name = ? limit 1;")
def select_one_return_dict(name):
    return name


@Select("select id, name, cnt from my_test where id = 1 limit 1;")
def select_without_param():
    pass


@SelectMany("select name, cnt from my_test where name = ? and cnt >= ? order by cnt desc;")
def select_many_by_name(name, cnt):
    return name, cnt

@SelectMany("select * from my_test where name in (:name) and cnt > :cnt limit :limit offset :offset")
def select_many_by_query_clauses(params: dict):
    return params

@SelectMany("select * from my_test where name in (:name) and cnt in (:cnt)")
def select_many_by_query_clauses2(params: dict):
    return params

@SelectMany("select name,count(*) from my_test where name in (:name) and cnt > :cnt group by :groupby order by :orderby")
def select_many_by_query_clauses3(params: dict):
    return params


@SelectMany("select name,count(*) from my_test where name in (:name) and cnt > :cnt group by :groupby having count(*) > :count_n order by :orderby")
def select_many_by_query_clauses4(params: dict):
    return params

@SelectMany("select name,count(*) from my_test where name in (:name) and cnt > :cnt group by :groupby having count(*) > :count_n order by :orderby limit :limit offset :offset")
def select_many_by_query_clauses5(params: dict):
    return params

@Update("update my_test set cnt = ? where name = ? limit ?;")
def update_cnt_by_name(name, cnt, limit=10):
    return cnt, name, limit


@Update("update my_test set cnt = ? where name in (?) limit ?;")
def update_cnt_by_name_and_in(name, cnt, limit=10):
    return cnt, name, limit

@Update("update my_test set cnt = :cnt where name in (:name) limit :limit;")
def update_cnt_by_word_placeholders(params:dict):
    return params


@Delete("delete from my_test where name = ? limit ?;")
def delete_by_name(name, limit=10):
    return name, limit

@Delete("delete from my_test where name in (?) and cnt in (?) limit ?;")
def delete_by_in(name, cnt, limit=10):
    return name, cnt, limit

@Delete("delete from my_test where name in (:name) and cnt = :cnt limit :limit;")
def delete_by_word_placeholders(params:dict):
    return params


def test_batch_insert():
    params = [{"name": "world", "cnt": 1}, {"name": "world", "cnt": 2}, {"name": "world", "cnt": 3}]
    assert batch_insert(params) == 3


def test_select_one():
    row = select_one_return_tuple("hello")
    assert row == (1, 'hello', 2)

    row = select_one_return_tuple_full()
    assert row == (2, 'hello', 3)

    row = select_one_by_in("world",[2,3])
    assert row == (4, 'world', 2)

    row = select_one_by_in_more_condition("world", [2, 3],1,1)
    assert row == (5, 'world', 3)

    params = {
        "cnt":[2,3],
        "name":"world",
        "limit":1,
        "offset":1,
    }
    row1 = select_one_by_word_placeholders(params=params)
    assert row == row1

    row = select_one_return_dict("hello")
    assert row == {'id': 1, 'name': 'hello', 'cnt': 2}
    assert select_without_param()["id"] == 1


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
    assert update_cnt_by_name_and_in(["update_one", "update_many"], 4, 2) == 2
    params = {
        "name":["update_one", "update_many"],
        "cnt":5,
        "limit":2,
    }
    assert update_cnt_by_word_placeholders(params=params) == 2
    assert select_many_by_name("update_many", 1) == []


def test_select_many_by_query_clauses():
    params = {
        "name": ["world", "hello"],
        "cnt": 1,
        "limit": 3,
        "offset": 1,
        # "orderby": "id desc",
        # "groupby": "name",
    }
    data = select_many_by_query_clauses(params=params)
    #print(data)
    assert len(data) == 3
    params = {
        "name": ["world", "hello"],
        "cnt": [1,2]
    }
    data = select_many_by_query_clauses2(params=params)
    # print(data)
    assert len(data) == 3

    params2 = {
        "name": ["world", "hello"],
        "cnt": 1,
        # "limit": 3,
        # "offset": 1,
        "orderby": "name desc",
        "groupby": "name",
    }
    data = select_many_by_query_clauses3(params=params2)
    # print(data)
    assert len(data) == 2

    params3 = {
        "name": ["world", "hello"],
        "cnt": 1,
        # "limit": 3,
        # "offset": 1,
        "orderby": "name desc",
        "groupby": "name",
        "count_n": 1,
    }
    data = select_many_by_query_clauses4(params=params3)
    # print(data)
    assert len(data) == 2

    params4 = {
        "name": ["world", "hello"],
        "cnt": 1,
        "limit": 1,
        "offset": 1,
        "orderby": "name desc",
        "groupby": "name",
        "count_n": 1,
    }
    data = select_many_by_query_clauses5(params=params4)
    # print(data)
    assert len(data) == 1



def test_delete_one():
    insert_with_param("delete_one", 1)
    insert_with_param("delete_one", 2)

    assert delete_by_name("delete_one", 1) == 1
    assert select_one_return_dict("delete_one")["cnt"] == 2

    insert_with_param("delete_one", 1)
    insert_with_param("delete_two", 1)
    insert_with_param("delete_two", 2)

    assert delete_by_in(["delete_one"], [1,2]) == 2
    params = {
        "cnt":2,
        "name":["delete_one","delete_two"],
        "limit": 1,
    }
    assert delete_by_word_placeholders(params=params) == 1


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
    assert select_one_return_dict("tx_rollback") is None


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
