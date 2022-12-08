from mysql_cli import insert, select


def _insert_param(param: dict):
    return tuple(param.values())


@insert("insert into test_table (a, b, c) values (%s, %s, %s);", _insert_param)
def insert_one(param: dict):
    pass


@select("select a, b, c from test_table where a = %s limit 1;")
def select_one(id):
    pass


def test_insert_one():
    insert_one({"a": 1, "b": 2, "c": 3})


def test_select_one():
    select_one(1)
