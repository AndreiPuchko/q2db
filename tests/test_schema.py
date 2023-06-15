if __name__ == "__main__":  # pragma: no cover
    import sys

    if "." not in sys.path:
        sys.path.insert(0, ".")


from q2db.schema import Q2DbSchema
from unittest.mock import patch, mock_open


def test_schema():
    # empty schema
    schema = Q2DbSchema()
    assert schema.schema == {"tables": {}, "indexes": {}}
    # wrong calls
    schema.add()
    schema.add({"column": ""})
    # add first column
    schema.add(table="topic_table", column="uid", datatype="int", datalen=9, pk=True)
    assert schema.schema == {
        "tables": {
            "topic_table": {
                "columns": {
                    "uid": {
                        "datatype": "int",
                        "datalen": 9,
                        "datadec": None,
                        "to_table": None,
                        "to_column": None,
                        "related": None,
                        "pk": True,
                        "ai": None,
                        "uk": None,
                        "index": None,
                    }
                },
                "indexes": {},
            }
        },
        "indexes": {},
    }
    schema.add(table="topic_table", column="name", datatype="varchar", datalen=100)
    assert schema.get_schema_table_attr("topic_table", "name") == {
        "datatype": "varchar",
        "datalen": 100,
        "datadec": None,
        "to_table": None,
        "to_column": None,
        "related": None,
        "pk": None,
        "ai": None,
        "uk": None,
        "index": None,
    }

    schema.add(table="message_table", column="uid", datatype="int", datalen=9, pk=True)
    schema.add(
        table="message_table",
        column="parent_uid",
        to_table="topic_table",
        to_column="uid",
        related="name",
    )
    schema.add(table="message_table", column="info", datatype="varchar", datalen=100)

    # the number of columns in the table 'topic_table'
    assert len(schema.get_schema_table_attr("topic_table")) == 2

    assert schema.get_schema_table_attr("topic_table", "name", "datalen") == 100

    # get list of child tables for table 'topic_table'
    assert schema.get_child_tables("topic_table", {"uid": 99}) == [
        {
            "child_table": "message_table",
            "child_column": "parent_uid",
            "parent_column": "uid",
            "parent_value": 99,
        }
    ]

    schema.add_index("message_table", "info, uid")
    assert len(schema.get_schema_indexes()) == 1
    schema.get_schema_tables()
    schema.get_schema_columns()
    schema.get_schema_attr()
    assert schema.get_primary_tables("message_table", {"parent_uid": 22}) == [
        {
            "primary_table": "topic_table",
            "primary_column": "uid",
            "child_column": "parent_uid",
            "child_value": 22,
        }
    ]
    with patch("builtins.open", mock_open(read_data='[{"column_name":"123"}]')) as filemock:
        schema.show_table("test.json")
    with patch("builtins.open", mock_open(read_data="""column_name\n123""")) as filemock:
        schema.show_table("test.csv")
    schema.show_table("123")

if __name__ == "__main__":  # pragma: no cover
    test_schema()
