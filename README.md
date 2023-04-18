# MySQL Python Client
A MySQL python client based on [mysql-connector-python](https://github.com/mysql/mysql-connector-python), with friendly interface and full-featured client configuration.  
Compare with the way which only takes table names and SQL parameters as input and assembles SQL statement internally, I personally prefer developer taking complete control of SQL, making it flexible which means you can use whatever SQL feature as you need, clear which means the final statement is obviously what you see in code and also no redundant part like '1=1' ever appear, also it's easier to debug and optimize as long as you can see the whole picture.

## Installation
### use pip
```bash
$ pip install mysql-cli
```
### use git repository
```text
# this is requirements.txt
# git+https://gitee.com/will4j/mysql-cli-py.git@main#egg=mysql-cli
git+https://github.com/will4j/mysql-cli-py.git@main#egg=mysql-cli 
```
```bash
$ pip install -r requirements.txt
```

## Usage
### Basic example
```python
import mysql_cli
from mysql_cli import Select


# init MySQL connection pool
mysql_cli.init_from_conf_file("tests/test_mysql.toml")


@Select("select id, name, cnt from my_test where name = ? limit 1;")
def select_one(name):
    return name
 
 
assert select_one("hello") == {'id': 1, 'name': 'hello', 'cnt': 2}
```

## Initialization
use `dict` or `toml` configuration file to init MySQL connection. you can find all options in [Python Connection Arguments](https://dev.mysql.com/doc/connector-python/en/connector-python-connectargs.html).
### Init from toml

```toml
# mysql_conf.toml
host = "127.0.0.1"
port = 3306
db = "my_db"
user = "root"
charset = "utf8mb4"
collation = "utf8mb4_general_ci"
pool_name = "my_pool"
pool_size = 5
# Important, for default is not do autocommit
autocommit = true
use_pure = true
```
```python
import mysql_cli


mysql_cli.init_from_conf_file("mysql_conf.toml")
```
### Init from dict
```python
import mysql_cli

mysql_conf = {
    "host": "127.0.0.1",
    "port": 3306,
    "db": "my_db",
    "user": "root",
    "charset": "utf8mb4",
    "collation": "utf8mb4_general_ci",
    "pool_name": "my_pool",
    "pool_size": 5,
    "autocommit": True,
    "use_pure": True,
}

mysql_cli.init_from_conf_dict(mysql_conf)
```

## Operate with connection
`mysql_cli.get_connection()`returns a `PooledMySQLConnection`
```python
import mysql_cli


with mysql_cli.get_connection() as cnx:
    with cnx.cursor() as cur:
      cur.execute("select * from my_test where id = ?", (1,))
      print(cur.fetchone())
```

## Query Decorators
All decorators use pooled connections for better performance, also use prepared statements to prevent SQL inject attacks.  
You can use ether Format (%s) or qmark (?) in SQL statement.  
For named format (like %(field_name)s) is not supported in prepared statement, SQL parameters should be formatted and returned by the decorated method as tuple.   
Decorators will take care of the creation and recycling of connections and cursors, as well as transactions, all you need to do is focusing on SQL business logics. 
### Insert
`Insert` execute single insert SQL and returns `lastrowid`.  
`BatchInsert` execute the insert SQL many times in one transaction and returns `affected_rows`.

```python
from mysql_cli import Insert, BatchInsert


@Insert("insert into my_test (name, cnt) values (?, ?);")
def insert(row: dict):
    return row["name"], row["cnt"]


@BatchInsert("insert into my_test (name, cnt) values (?, ?);")
def batch_insert(rows):
    return tuple((row["name"], row["cnt"]) for row in rows)


assert insert({"name": "hello", "cnt": 2}) == 1 # lastrowid
params = [{"name": "world", "cnt": 1}, {"name": "world", "cnt": 2}]
assert batch_insert(params) == 2 # affected_rows
```
### Select
`Select` execute select SQL and return one row.  
`SelectMany` execute select SQL and return list of rows.

support use ":word" or ? as a placeholder, but when the use of ":word" placeholder does not allow to use "?" as a placeholder

Returned row is dictionary by default, you can set `dictionary=False` to return row as tuple.
```python
from mysql_cli import Select, SelectMany


@Select("select id, name, cnt from my_test where name = ? limit 1;", dictionary=False)
def select_one_return_tuple(name):
    return name


@Select("select id, name, cnt from my_test where name = ? limit 1;")
def select_one_return_dict(name):
    return name


@SelectMany("select name, cnt from my_test where name = ? order by cnt asc;")
def select_many_by_name(name):
    return name

@Select("select id, name, cnt from my_test where name = ? and cnt in (?) limit 1;", dictionary=False)
def select_one_by_in(name,cnt):
    return name,cnt

@Select("select id, name, cnt from my_test where name = ? and cnt in (?) limit ? offset ?;", dictionary=False)
def select_one_by_in_more_condition(name,cnt,limit,offset):
    return name,cnt,limit,offset

@Select("select id, name, cnt from my_test where name = :name and cnt in (:cnt) limit :limit offset :offset;", dictionary=False)
def select_one_by_word_placeholders(params: dict):
    return params


row = select_one_return_tuple("hello")
assert row == (1, 'hello', 2)
row = select_one_return_dict("hello")
assert row == {'id': 1, 'name': 'hello', 'cnt': 2}
rows = select_many_by_name("hello")
assert len(rows) == 2
assert rows[0] == {'id': 1, 'name': 'hello', 'cnt': 2}
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
```
### Update
`Update` execute update SQL and return affected row number.

support use ":word" or ? as a placeholder, but when the use of ":word" placeholder does not allow to use "?" as a placeholder

```python
from mysql_cli import Update


@Update("update my_test set cnt = ? where name = ? limit ?;")
def update_cnt_by_name(name, cnt, limit=10):
    return cnt, name, limit

@Update("update my_test set cnt = ? where name in (?) limit ?;")
def update_cnt_by_name_and_in(name, cnt, limit=10):
    return cnt, name, limit

@Update("update my_test set cnt = :cnt where name in (:name) limit :limit;")
def update_cnt_by_word_placeholders(params:dict):
    return params


assert update_cnt_by_name("update_many", 0) == 3 # affected_rows
assert update_cnt_by_name_and_in(["update_one", "update_many"], 4, 2) == 2
params = {
    "name":["update_one", "update_many"],
    "cnt":5,
    "limit":2,
}
assert update_cnt_by_word_placeholders(params=params) == 2
```
### Delete
`Delete` execute delete SQL and return affected row number.

support use ":word" or ? as a placeholder, but when the use of ":word" placeholder does not allow to use "?" as a placeholder
```python
from mysql_cli import Delete


@Delete("delete from my_test where name = ? limit ?;")
def delete_by_name(name, limit=10):
    return name, limit

@Delete("delete from my_test where name in (?) and cnt in (?) limit ?;")
def delete_by_in(name, cnt, limit=10):
    return name, cnt, limit

@Delete("delete from my_test where name in (:name) and cnt = :cnt limit :limit;")
def delete_by_word_placeholders(params:dict):
    return params

assert delete_by_name("delete_many") == 2 # affected_rows
assert delete_by_in(["delete_one"], [1,2]) == 2
params = {
    "cnt":2,
    "name":["delete_one","delete_two"],
    "limit": 1,
}
assert delete_by_word_placeholders(params=params) == 1
```

### Transactional
`Transactional` will start a SQL transaction and control commit or rollback based on decorated method's return. Connection and cursor will be shared in thread local among query decorators. If `Transactional` appears multiple times, they will be merged into one transaction, the very first `Transactional` decides whether to commit or rollback transaction.

```python
from mysql_cli import BatchInsert, Select, Transactional


@BatchInsert("insert into my_test (name, cnt) values (?, ?);")
def batch_insert(params):
    return tuple((row["name"], row["cnt"]) for row in params)


@Select("select id, name, cnt from my_test where name = ? limit 1;")
def select_one_return_dict(name):
    return name


@Transactional
def transaction_rollback():
    params = [{"name": "tx_rollback", "cnt": 1}, {"name": "tx_rollback", "cnt": 2}]
    batch_insert(params)

    assert select_one_return_dict("tx_rollback")["name"] == "tx_rollback"
    raise RuntimeError("rollback")


transaction_rollback()
assert select_one_return_dict("tx_rollback") is None
```


## References
1. https://dev.mysql.com/doc/connector-python/en/connector-python-api-mysqlcursorprepared.html
