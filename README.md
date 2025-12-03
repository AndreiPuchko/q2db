[![Python application](https://github.com/AndreiPuchko/q2db/actions/workflows/main.yml/badge.svg)](https://github.com/AndreiPuchko/q2db/actions/workflows/main.yml)

# Q2DB: Lightweight Python DB API Wrapper & ORM

Q2DB is a lightweight Python library that wraps standard DB API connectors for MySQL, PostgreSQL, and SQLite, providing a simple ORM-like interface for database operations. It enables easy schema definition and migration, supports basic CRUD operations, and offers a convenient cursor abstraction for querying and manipulating data. Q2DB is designed for rapid prototyping, small projects, and educational purposes, focusing on simplicity and minimal configuration.

Key features include:
- Unified API for MySQL, PostgreSQL, and SQLite
- Schema definition and migration (ADD COLUMN only)
- Simple ORM-like CRUD operations (insert, update, delete, get)
- Foreign key checks and logging of changes
- Cursor abstraction for flexible queries and record navigation
- Docker support for quick setup and testing


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
