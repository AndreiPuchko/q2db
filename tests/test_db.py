if __name__ == "__main__":
    import sys

    if "." not in sys.path:
        sys.path.insert(0, ".")

from q2db.db import Q2Db
from q2db.schema import Q2DbSchema


def prepare_dataset(database):
    schema = Q2DbSchema()
    schema.add(table="topic_table", column="uid", datatype="int", datalen=9, pk=True)
    schema.add(table="topic_table", column="name", datatype="varchar", datalen=100)

    schema.add(table="message_table", column="uid", datatype="int", datalen=9, pk=True)
    schema.add(table="message_table", column="message", datatype="varchar", datalen=100)
    schema.add(
        table="message_table", column="parent_uid", to_table="topic_table", to_column="uid", related="name"
    )

    schema.add_index("message_table", "message, uid")

    database.cursor("drop table if exists topic_table")
    database.cursor("drop table if exists log_topic_table")
    database.cursor("drop table if exists message_table")
    database.cursor("drop table if exists log_message_table")

    database.set_schema(schema)

    assert database.migrate_error_list == []

    assert database.insert("topic_table", {"name": "topic 0"})
    assert database.insert("topic_table", {"name": "topic 1"})
    assert database.insert("topic_table", {"name": "topic 2"})
    assert database.insert("topic_table", {"name": "topic 3"})

    assert database.insert("message_table", {"message": "Message 0 in 0", "parent_uid": 0})
    assert database.insert("message_table", {"message": "Message 1 in 0", "parent_uid": 0})
    assert database.insert("message_table", {"message": "Message 0 in 1", "parent_uid": 1})
    assert database.insert("message_table", {"message": "Message 1 in 1", "parent_uid": 1})

    # cursor = database.cursor(table_name="message_table")
    # assert cursor.row_count() == 4

    assert not database.insert("message_table", {"message": "Message 1 in 1", "parent_uid": 99})


def _test_get(database):
    prepare_dataset(database)

    assert (
        database.get_tables().sort()
        == ["topic_table", "log_topic_table", "message_table", "log_message_table"].sort()
    )
    assert database.get("topic_table", "uid=1", "name") == "topic 1"
    assert database.get("topic_table", "name='topic 1'", "name") == "topic 1"

    assert database.get("message_table", "uid=0 and parent_uid=0", "message") == "Message 0 in 0"
    assert database.get("message_table", "uid=1 and parent_uid=0", "message") == "Message 1 in 0"
    assert database.get("message_table", "uid=2 and parent_uid=0", "message") == {}
    assert database.get("message_table", "uid=2", "message") == "Message 0 in 1"


def _test_update(database):
    prepare_dataset(database)
    new_name = "updated topic 0"
    assert database.update("topic_table", {"uid": 0, "name": new_name})
    assert database.get("topic_table", "uid=0", "name") == new_name

    assert database.update("message_table", {"uid": 0, "message": new_name})
    assert database.get("message_table", "uid=0", "message") == new_name

    assert not database.update("message_table", {"uid": 0, "parent_uid": 22})


def _test_cursor(database):
    prepare_dataset(database)

    cursor = database.cursor(table_name="topic_table")
    assert cursor.row_count() == 4
    record = cursor.record(3)
    assert record["uid"] == "3"
    assert cursor.row_count() == 4
    assert not cursor.delete({"uid": 1})
    assert cursor.delete({"uid": 2})
    assert cursor.row_count() == 3
    assert cursor.delete({"uid": 99})
    assert cursor.row_count() == 3
    assert (len([x for x in cursor.records()])) == 3

    assert cursor.record(0)["name"] == "topic 0"
    cursor.set_order("name desc").refresh()
    assert cursor.record(0)["name"] == "topic 3"

    cursor.set_where(" name like '%3%' ").refresh()
    assert cursor.row_count() == 1

    cursor.set_where(" name not like '%3%' ").refresh()
    assert cursor.row_count() == 2

    assert database.cursor(sql="select name from topic_table").record(0)["name"] == "topic 0"


def _test_delete(database):
    prepare_dataset(database)
    assert not database.delete("topic_table", {"uid": 0})
    assert database.delete("topic_table", {"uid": 2})
    assert database.delete("topic_table", {"uid": 123})


def _all_tests(database):
    _test_get(database)
    print("  get done")

    _test_cursor(database)
    print("  cursor done")

    _test_update(database)
    print("  update done")

    _test_delete(database)
    print("  delete")


def test_sqlite():
    print("sqlite start")
    database = Q2Db()
    _all_tests(database)
    database.close()
    database = None
    print("sqlite done")


def test_postgresql():
    print("postgresql start")
    database = Q2Db(
        "postgresql",
        database_name="q2test",
        host="0.0.0.0",
        port=5432,
        user="q2user",
        password="q2test",
    )
    _all_tests(database)
    database.close()
    database = None
    print("postgresql done")


def test_mysql():
    print("mysql start")
    database = Q2Db(url="mysql://root:q2test@0.0.0.0:3308/q2test")

    _all_tests(database)
    print("mysql done")
    database.close()
    database = None


if __name__ == "__main__":
    test_mysql()
    test_postgresql()
    test_sqlite()
    # for x in [x for x in (locals().keys())]:
    #     if x.startswith("test") and type(locals()[x]).__name__ == "function":
    #         print(x)
    #         locals()[x]()
