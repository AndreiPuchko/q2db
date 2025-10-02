if __name__ == "__main__":
    import sys

    sys.path.insert(0, ".")

from q2db.schema import Q2DbSchema
from q2db.db import Q2Db
from q2db.cursor import Q2Cursor


def print_cursor(cursor):
    print(f"+--------- {cursor.table_name} === {cursor.row_count()} records, where = '{cursor.where}'")
    for x in cursor.records():
        print("|", x)
    print(f"+--end of- {cursor.table_name} ===")


def demo(demo_database: Q2Db):
    if demo_database.connection is None:
        raise Exception("No database connection")

    demo_database.cursor("drop table if exists topic_table")
    demo_database.cursor("drop table if exists log_topic_table")
    demo_database.cursor("drop table if exists message_table")
    demo_database.cursor("drop table if exists log_message_table")

    schema = Q2DbSchema()
    schema.add(table="topic_table", column="uid", datatype="int", datalen=0, pk="*")
    schema.add(table="topic_table", column="name", datatype="varchar", datalen=100)

    schema.add(table="message_table", column="uid", datatype="int", datalen=9, pk=True, ai="*")
    schema.add(table="message_table", column="message", datatype="varchar", datalen=100)
    schema.add(
        table="message_table",
        column="parent_uid",
        to_table="topic_table",
        to_column="uid",
        related="name",
    )

    demo_database.set_schema(schema)  # create tables in database

    demo_database.cursor("alter table message_table add column f6 char(25)")

    demo_database.migrate_schema()

    print(demo_database.migrate_error_list)
    print(demo_database.get_tables())

    demo_database.insert("topic_table", {"name": "0" * 50, "uid": 99})
    demo_database.insert("topic_table", {"name": "1" * 50})
    demo_database.insert("topic_table", {"name": "2" * 50})
    demo_database.insert("topic_table", {"name": "3" * 50})

    # return

    print_cursor(demo_database.cursor(table_name="topic_table"))
    print("update row - uid=2")
    demo_database.update("topic_table", {"uid": "2", "name": "------------"})
    print_cursor(demo_database.cursor(table_name="topic_table", where="uid = 2"))

    cursor = demo_database.cursor(table_name="topic_table")
    print("before delete")
    print_cursor(cursor)
    demo_database.delete("topic_table", {"uid": "2"})
    print("after db.delete but without cursor.refresh()")
    print_cursor(cursor)
    print("after cursor.refresh()")
    cursor.refresh()
    print_cursor(cursor)

    cursor.delete({"uid": "0"})
    print_cursor(cursor)
    print("print row 1")
    print(cursor.record(1))
    print("print row 2")
    print(cursor.record(2))

    print_cursor(demo_database.cursor(table_name="log_topic_table"))

    if not demo_database.insert("message_table", {"message": "just message", "parent_uid": 1}):
        print(demo_database.last_sql_error)
        print(demo_database.last_error_data)
        raise Exception("message_table inser error")

    if not demo_database.insert("message_table", {"message": "just message", "parent_uid": 135}):
        print(demo_database.last_sql_error)
        print(demo_database.last_error_data)

    if not demo_database.update("message_table", {"message": "just message", "parent_uid": 135, "uid": 0}):
        print(demo_database.last_sql_error)
        print(demo_database.last_error_data)

    if not demo_database.update("message_table", {"message": "just message!!", "uid": 0}):
        print(demo_database.last_sql_error)
        print(demo_database.last_error_data)

    print_cursor(demo_database.cursor(table_name="message_table"))

    if not demo_database.delete("topic_table", {"uid": "1"}):
        print(demo_database.last_sql_error)
        print(demo_database.last_error_data)

    cu1 = demo_database.cursor(sql="select * from topic_table where name like '%2%' order by name")
    print(cu1.row_count())
    print(cu1.record(0))

    cursor = demo_database.cursor(table_name="topic_table").set_where(" name like '%2%' ").refresh()
    print_cursor(cursor)

    cursor = demo_database.table(table_name="topic_table")

    cursor.set_current_row(1)
    print(cursor.current_row())
    print(f"row={cursor.current_row()},uid={cursor.r.uid}, name={cursor.r.name}")
    print_cursor(cursor)
    print(cursor.insert({"name": "name name"}))
    for x in range(4):
        cursor.insert({"name": f"name {x}"})
    print_cursor(cursor)

    print(cursor.get_columns())

    cursor.set_current_row(0)
    print(cursor.r.name)

    cursor.first()
    print(cursor.r.name)
    print(cursor.bof())

    cursor.last()
    print(cursor.r.name)
    print(cursor.bof())

    cursor.prev()
    print(cursor.r.name)

    cursor.next()
    print(cursor.r.name)
    cursor.next()
    print(cursor.r.name)
    print(cursor.eof())

    c: Q2Cursor = cursor
    print_cursor(c)
    print(c.seek_row({"uid": "4"}))
    c.export_json("temp/export_test.json")
    c.export_csv("temp/export_test.csv")
    print(c.row_count())
    c.import_json("temp/export_test.json")
    c.import_csv("temp/export_test.csv")
    print(c.row_count())
    c.set_order("name").refresh()
    print_cursor(cursor)

    Q2DbSchema.show_table("temp/export_test.json")
    Q2DbSchema.show_table("temp/export_test.csv")
    # print(demo_database.get_tables())
    print(schema.get_schema_table_attr("message_table").keys())
    print(schema.get_schema_table_attr("message_table").get("f6"))

    print("==")
    cu1 = demo_database.cursor(sql="select * from topic_table where name like '%2%' order by name")
    print(demo_database.last_sql)
    print(cu1.row_count())
    print(cu1.record(0))

    cu1 = demo_database.cursor(sql="select * from topic_table where name like %s order by name", data=["%2%"])
    print(demo_database.last_sql)
    print(demo_database.last_record)
    print(cu1.row_count())
    print(cu1.record(0))


if __name__ == "__main__":
    demo(Q2Db("sqlite3", database_name=":memory:"))
    # demo(Q2Db("sqlite3", database_name="temp/q2dbtest.sqlite"))
