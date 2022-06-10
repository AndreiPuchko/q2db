[![Python application](https://github.com/AndreiPuchko/q2db/actions/workflows/main.yml/badge.svg)](https://github.com/AndreiPuchko/q2db/actions/workflows/main.yml)
# The light Python DB API wrapper with some ORM functions (MySQL, PostgreSQL, SQLite)
## Quick start (run demo files)
## - in docker:
```bash
git clone https://github.com/AndreiPuchko/q2db && cd q2db/database.docker
./up.sh
./down.sh
```  
## - on your system:
```bash
pip install q2db
git clone https://github.com/AndreiPuchko/q2db && cd q2db
# sqlite:
python3 ./demo/demo.py
# mysql and postgresql:
pip install mysql-connector-python psycopg2-binary
pushd database.docker && docker-compose up -d && popd
python3 ./demo/demo_mysql.py
python3 ./demo/demo_postgresql.py
pushd database.docker && docker-compose down -v && popd
```
# Features:
 ---
## Connect
```python
from q2db.db import Q2Db

database_sqlite = Q2Db("sqlite3", database_name=":memory:")
# or just
database_sqlite = Q2Db()


database_mysql = Q2Db(
    "mysql",
    user="root",
    password="q2test"
    host="0.0.0.0",
    port="3308",
    database_name="q2test",
)
# or just
database_mysql = Q2Db(url="mysql://root:q2test@0.0.0.0:3308/q2test")

database_postgresql = Q2Db(
    "postgresql",
    user="q2user",
    password="q2test"
    host="0.0.0.0",
    port=5432,
    database_name="q2test1",
)
```
---
## Define & migrate database schema (ADD COLUMN only).
```python
q2db.schema import Q2DbSchema

schema = Q2DbSchema()

schema.add(table="topic_table", column="uid", datatype="int", datalen=9, pk=True)
schema.add(table="topic_table", column="name", datatype="varchar", datalen=100)

schema.add(table="message_table", column="uid", datatype="int", datalen=9, pk=True)
schema.add(table="message_table", column="message", datatype="varchar", datalen=100)
schema.add(
    table="message_table",
    column="parent_uid",
    to_table="topic_table",
    to_column="uid",
    related="name"
)

database.set_schema(schema)
```
---
## INSERT, UPDATE, DELETE
```python
database.insert("topic_table", {"name": "topic 0"})
database.insert("topic_table", {"name": "topic 1"})
database.insert("topic_table", {"name": "topic 2"})
database.insert("topic_table", {"name": "topic 3"})

database.insert("message_table", {"message": "Message 0 in 0", "parent_uid": 0})
database.insert("message_table", {"message": "Message 1 in 0", "parent_uid": 0})
database.insert("message_table", {"message": "Message 0 in 1", "parent_uid": 1})
database.insert("message_table", {"message": "Message 1 in 1", "parent_uid": 1})

# this returns False because there is no value 2 in topic_table.id - schema works!
database.insert("message_table", {"message": "Message 1 in 1", "parent_uid": 2})


database.delete("message_table", {"uid": 2})

database.update("message_table", {"uid": 0, "message": "updated message"})
```
---
## Cursor
```python
cursor = database.cursor(table_name="topic_table")
cursor = database.cursor(
    table_name="topic_table",
    where=" name like '%2%'",
    order="name desc"
)
cursor.insert({"name": "insert record via cursor"})
cursor.delete({"uid": 2})
cursor.update({"uid": 0, "message": "updated message"})

cursor = database.cursor(sql="select name from topic_table")

for x in cursor.records():
    print(x)
    print(cursor.r.name)

cursor.record(0)['name']
cursor.row_count()
cursor.first()
cursor.last()
cursor.next()
cursor.prev()
cursor.bof()
cursor.eof()
```