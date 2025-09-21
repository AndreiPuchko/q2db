if __name__ == "__main__":  # pragma: no cover
    import sys

    if "." not in sys.path:
        sys.path.insert(0, ".")

from q2db.db import Q2Db

from q2db.cursor import Q2Cursor, Q2MysqlCursor, Q2PostgresqlCursor, Q2SqliteCursor
from q2db.schema import Q2DbSchema
from q2db.utils import num
import pytest
from unittest.mock import patch, mock_open


def prepare_dataset(database: Q2Db):
    schema = Q2DbSchema()
    schema.add(table="topic_table", column="uid", datatype="int", datalen=9, pk=True)
    schema.add(table="topic_table", column="name", datatype="varchar", datalen=100)
    schema.add(table="topic_table", column="price", datatype="dec", datalen=15, datadec=2)
    schema.add(table="topic_table", column="price1", datatype=None, datalen=15, datadec=2)
    schema.add(table="topic_table", column="date", datatype="date")
    schema.add(table="topic_table", column="notes", datatype="text")

    schema.add(table="message_table", column="uid", datatype="int", datalen=9, pk=True)
    schema.add(table="message_table", column="message", datatype="varchar", datalen=100)
    schema.add(
        table="message_table", column="parent_uid", to_table="topic_table", to_column="uid", related="name"
    )

    schema.add_index("message_table", "message, uid")

    schema.add(table="just_table", column="uid", datatype="int", datalen=9, pk=True, ai=True)
    schema.add(table="just_table", column="data", datatype="int", datalen=9)

    schema.add(table="just_table2", column="uid", datatype="char", datalen=9, pk=True)
    schema.add(table="just_table2", column="name", datatype="char", datalen=10)

    database.cursor("drop table if exists topic_table")
    database.cursor("drop table if exists log_topic_table")
    database.cursor("drop table if exists message_table")
    database.cursor("drop table if exists log_message_table")
    database.cursor("drop table if exists just_table")
    database.cursor("drop table if exists log_just_table")
    database.cursor("drop table if exists just_table2")
    database.cursor("drop table if exists log_just_table2")

    database.set_schema(schema)

    assert database.migrate_error_list == []

    assert database.insert("topic_table") is False

    assert database.insert("topic_table", {"name": "topic 0"})
    assert database.insert("topic_table", {"name": "topic 1"})
    assert database.insert("topic_table", {"name": "topic 2"})
    assert database.insert("topic_table", {"name": "topic 3"})

    database.transaction()
    assert database.cursor("select count(*) as row_count from topic_table").r.row_count == "4"
    assert not database.raw_insert("topic_table")
    assert not database.raw_insert()
    assert not database.raw_insert("topic_table1", {"f1": 1})
    if database.db_engine_name != "postgresql":
        assert not database.raw_insert("topic_table", {"name": "topic 4", "uid": "456,="})
    assert database.raw_insert("topic_table", {"name": "topic 4", "uid": 4})
    database.rollback()
    assert database.cursor("select count(*) as row_count from topic_table").r.row_count == "4"

    assert database.insert("message_table", {"message": "Message 0 in 0", "parent_uid": 0})
    assert database.insert("message_table", {"message": "Message 1 in 0", "parent_uid": 0})
    assert database.insert("message_table", {"message": "Message 0 in 1", "parent_uid": 1})
    assert database.insert("message_table", {"message": "Message 1 in 1", "parent_uid": 1})

    # cursor = database.cursor(table_name="message_table")
    # assert cursor.row_count() == 4

    assert not database.insert("message_table", {"message": "Message 1 in 1", "parent_uid": 99})

    assert database.insert("just_table", {"message": "Message 1 in 1", "uid": 99})

    assert database.insert("just_table2", {"uid": 33, "name": "1"})
    assert database.insert("just_table2", {"uid": 33, "name": "2"})

    database.db_schema = None
    database.migrate_schema()
    assert database.get_primary_key_columns("topic_table").get("uid") is not None
    database.set_schema(schema)
    database.guest_mode = True
    database.migrate_schema()
    database.guest_mode = False


def _test_get(database: Q2Db):
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
    assert database.get("just_table2", "uid=2+'xx'", "message") == ""

    assert not database.get_uniq_value("just_table2", "w1", 0)
    assert database.get_uniq_value("just_table2", "uid", "44") == "45"

    assert database.get_uniq_value("just_table", "uid", 15) == 15
    assert database.get_uniq_value("just_table", "uid", 1) == 2


def _test_update(database: Q2Db):
    prepare_dataset(database)
    new_name = "updated topic 0"
    database.transaction()
    assert database.update("topic_table", {"uid": 0, "name": new_name, "price": ""})
    database.rollback()
    assert database.get("topic_table", "uid=0", "name") != new_name

    database.transaction()
    assert database.update("topic_table", {"uid": 0, "name": new_name, "price": ""})
    assert database.get("topic_table", "uid=0", "name") == new_name
    database.commit()
    assert database.get("topic_table", "uid=0", "name") == new_name

    assert database.update("message_table", {"uid": 0, "message": new_name})
    assert database.get("message_table", "uid=0", "message") == new_name

    assert not database.update("message_table", {"uid": 0, "parent_uid": 22})

    assert not database.update("just_table2")
    assert not database.update("just_table2", {"d1": 0})
    if database.db_engine_name != "sqlite3":
        assert not database.update("just_table", {"uid": "q2", "data": 1})


def _test_cursor(database: Q2Db):
    prepare_dataset(database)

    cursor = database.cursor(table_name="topic_table")
    assert cursor.row_count() == 4
    record = cursor.record(3)
    assert record["uid"] == "3"
    assert cursor.row_count() == 4
    assert not cursor.delete({"uid": 1})
    assert cursor.delete({"uid": 2})
    assert cursor.row_count() == 3
    assert not cursor.delete({"uid": 99})
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

    cursor = database.cursor("select *, from topic_table")
    cursor.last_sql()
    cursor.last_sql_error()
    assert cursor.get("w1=3") == {}
    cursor.sub_filter("name", "to+1")

    cursor.prepare_column_search("name", "to+1", "after")

    assert cursor.get_next_sequence("uid", 15) == 0


def _test_delete(database: Q2Db):
    prepare_dataset(database)
    assert not database.delete("topic_table", {"uid": 0})
    assert not database.delete("topic_table")
    assert database.delete("topic_table", {"uid": 2})
    assert not database.delete("topic_table", {"uid": 123})

    assert not database.delete("just_table2", {"uid": 123})
    assert database.table("just_table2").row_count() == 2

    assert database.delete("just_table2", {"uid": "33"})
    assert database.table("just_table2").row_count() == 1

    assert not database.delete("just_table2", {"name1": "2"})
    assert database.table("just_table2").row_count() == 1

    assert database.delete("just_table2", {"name": "2"})
    assert database.table("just_table2").row_count() <= 0


def _test_table(database: Q2Db):
    prepare_dataset(database)
    # cursor = database.cursor("select * from topic_table")
    # assert cursor.row_count() == 4

    table = database.table("topic_table")
    assert table.row_count() == 4
    assert table._rows[0].get("uid") == "0"
    assert table._rows[1].get("uid") == "1"
    assert table._rows.fetch_row(999).get("uid") is None

    assert table.r.uid == "0"
    table.next()
    assert table.r.uid == "1"
    table.prev()
    assert table.r.uid == "0"
    table.last()
    assert table.r.uid == "3"
    assert table.record().get("uid") == "3"
    assert table.get_record().get("uid") == "3"
    table.first()
    assert table.r.uid == "0"
    assert table.bof()
    table.set_current_row(1)
    assert table.r.uid == "1"
    table.set_current_row(3)
    assert table.eof()
    table.set_current_row(-1)
    assert table.r.uid == "0"
    table.set_current_row(9999)
    assert table.r.uid == "3"

    assert table.get("uid=0", "uid") == "0"
    assert table.row_count() == 4

    table.transaction()
    table.insert({"name": 123})
    table.last_record()
    table.commit()
    assert table.row_count() == 5

    table.transaction()
    table.insert({"name": 789})
    table.rollback()
    table.refresh()
    assert table.row_count() == 5
    # print(table._rows)
    table.set_order("name")
    table.set_where("uid=3")
    table.refresh()
    assert table.row_count() == 1
    table.set_where("uid<>3")
    assert len(table.sub_filter("name", "to-1")) == 2
    assert len(table.sub_filter("name", "to+1")) == 1
    assert len(table.sub_filter("name", "1*12")) == 2

    table.set_order("uid")
    table.update({"uid": 0, "name": "top"})
    table.first()
    assert table.r.name == "top"
    table.raw_insert({"uid": 99, "name": "top 99"})
    table.set_where("uid = 99")
    table.refresh()
    assert table.r.name == "top 99"

    table.set_where("price = 33")
    table.refresh()
    table.insert({"name": "last record"})
    table.set_where("name = 'last record'")
    table.refresh()
    assert num(table.r.price) == num("33.00")

    table.set_where()
    table.refresh()
    assert table.seek_primary_key_row({"uid": 3}) == 3

    if database.db_engine_name == "sqlite3":
        assert table.seek_row({"price": "33", "name": "last record"}) == 5
    else:
        assert table.seek_row({"price": "33.00", "name": "last record"}) == 5

    assert table.seek_row({"price": "99.00", "name": "last record"}) == 7
    assert table.get_primary_key_columns() == ["uid"]
    assert table.get_uniq_value("uid", 15) == 15

    assert table.get_next_sequence("uid", 15) == 6
    assert table.get_next_sequence("uid", 15) == 6

    table.set_where("name like '%top%'")
    table.refresh()
    # for x in table.records():
    #     print(x)
    assert table.get_next_sequence("uid", 15) == 4

    table.set_where()
    table.refresh()

    assert table.record(0, ["uid"]).get("uid") == "0"
    assert table.record(99) == {}

    table.get_columns()


def _test_impexp(database: Q2Db):
    table = database.table("topic_table")
    assert table.row_count() == 7
    with patch("builtins.open", mock_open(read_data='[{"name":"json import", "price": "19"}]')) as filemock:
        table.import_json("file.json")
        table.import_json(open("file.json"))
    assert table.row_count() == 9

    with patch("builtins.open", mock_open(read_data="""name,price\ncsv import, 18""")) as filemock:
        table.import_csv("file.csv")
        table.import_csv(open("file.csv"))
    assert table.row_count() == 11
    # bad file
    if database.db_engine_name != "sqlite3":
        with pytest.raises(Exception, match="Import error:.*") as e:
            with patch(
                "builtins.open", mock_open(read_data='[{"name":"json import", "price": "19+w"}]')
            ) as filemock:
                table.import_json("file.json")
        assert table.row_count() == 11

        with pytest.raises(Exception, match="Import error:.*") as e:
            with patch("builtins.open", mock_open(read_data="""name;price\ncsv import; 18+w""")) as filemock:
                table.import_csv("file.csv")
        assert table.row_count() == 11

    with patch("builtins.open", mock_open()) as filemock:
        table.export_csv("file.csv")

    with patch("builtins.open", mock_open()) as filemock:
        table.export_json("file.json")
        table.export_json(open("file.json"))


def _all_tests(database):
    _test_get(database)
    print("  get done")

    _test_cursor(database)
    print("  cursor done")

    _test_update(database)
    print("  update done")

    _test_delete(database)
    print("  delete done")

    _test_cursor(database)
    print("  cursor done")

    _test_table(database)
    print("  table done")

    _test_impexp(database)
    print("imp exp done")


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
        host="localhost",
        port=6432,
        user="q2user",
        password="q2test",
    )
    _all_tests(database)
    database.close()
    database = None
    print("postgresql done")


def test_mysql():
    print("mysql start")
    database = Q2Db(url="mysql://root:q2test@localhost:3308/q2test")
    _all_tests(database)
    print("mysql done")
    database.close()
    database = None


def test_wrongdb():
    print("wrongdb start")
    # Wrong engine
    with pytest.raises(Exception, match="Sorry, wrong DBAPI engine - .*") as e:
        assert Q2Db(url="mysql21://root:q2test@localhost:3308/q2test", create_only=True)
    # User doesnt exist, create and open
    assert Q2Db(url="mysql://root1:q2test2@localhost:3308/q2test", root_user="root", root_password="q2test")

    # Wrong passwort for root1
    with pytest.raises(Exception, match="Access denied for user ") as e:
        Q2Db(url="mysql://root1:q2test1@localhost:3308/q2test", root_user="root", root_password="q2test")

    Q2Db(url="mysql://root:q2test@localhost:3308/q2test").cursor("DROP USER IF EXISTS root1")

    Q2Db(url="mysql://root:q2test@localhost:3308/q2test", create_only=True)
    Q2Db(url="postgresql://q2user:q2test@localhost:6432/q2test", create_only=True)

    with pytest.raises(Exception, match="Access denied for user ") as e:
        assert Q2Db(
            url="mysql://root:q2test@localhost:3308/q2test",
            create_only=True,
            get_admin_credential_callback=lambda *argc: ("12", "34"),
        )
    database = Q2Db(url="mysql://root:q2test@localhost:3308/q2test")

    database = Q2Db(url="sqlite://root:q2test@localhost:3308/:memory:")
    print("wrongdb done")
    database.close()
    database = None

    Q2Cursor.get_table_names_sql()
    Q2Cursor.get_table_columns_sql()
    Q2MysqlCursor.get_table_names_sql()
    Q2MysqlCursor.get_table_columns_sql()
    Q2PostgresqlCursor.get_table_names_sql()
    Q2PostgresqlCursor.get_table_columns_sql()
    Q2SqliteCursor.get_table_names_sql()
    Q2SqliteCursor.get_table_columns_sql()


if __name__ == "__main__":  # pragma: no cover
    test_mysql()
    test_postgresql()
    test_sqlite()
    test_wrongdb()
