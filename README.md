# mysql-cli-py
Mysql python client.

## Usage
### Basic example
```python
import mysql_cli
from mysql_cli import Select


# init mysql
mysql_cli.init_from_conf_file("tests/test_mysql.toml")


@Select("select id, name, cnt from my_test where name = %s limit 1;")
def select_one(name):
    return name
 
 
assert select_one("hello") == {'id': 1, 'name': 'hello', 'cnt': 2}
```

## Operate with connection
`mysql_cli.get_connection()`returns a `PooledMySQLConnection`
```python
import mysql_cli


with mysql_cli.get_connection() as cnx:
    with cnx.cursor() as cur:
      cur.execute("select * from my_test where id = %s", (1,))
      print(cur.fetchone())
```

## Query Decorators
All decorators use pooled connections, as well as prepared statements to prevent SQL inject attacks.
### Insert
### Select
### Update
### Delete
