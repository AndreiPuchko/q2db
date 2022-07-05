#    Copyright (C) 2021 Andrei Puchko
#
#    Licensed under the Apache License, Version 2.0 (the "License");
#    you may not use this file except in compliance with the License.
#    You may obtain a copy of the License at
#
#        http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS,
#    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#    See the License for the specific language governing permissions and
#    limitations under the License.
"""

"""


if __name__ == "__main__":
    import sys

    sys.path.insert(0, ".")

    from tests import test_schema

    test_schema.test_schema()

    # from demo import demo_sqlite

    # demo_sqlite.demo()
import json
import csv


class Q2DbSchema:
    """
    {"tables": {"table_name": {"columns": {}, "indexes": {...}}}}
    """

    def __init__(self, schema={}):
        self.schema = {"tables": {}, "indexes": {}}

    def add(
        self,
        table="",
        column="",
        datatype="char",
        datalen=None,
        datadec=None,
        to_table=None,
        to_column=None,
        related=None,
        pk=None,
        ai=None,
        uk=None,
        index=None,
    ):
        """
        :param table: database table name

        :param column: column name
        :param datatype: type
        :param datalen: lenght
        :param datadec: decimal precison

        :param to_table: foreign key table
        :param to_column: foreign key column
        :param related: foreign column to show

        :param pk: primary key
        :param ai: autoincrement
        :param uk: unique
        :param index: create index for the column
        """
        if not (table or column):
            return

        if isinstance(table, dict):
            column = table.get("column")
            datatype = table.get("datatype")
            datalen = table.get("datalen")
            datadec = table.get("datadec")
            to_table = table.get("to_table")
            to_column = table.get("to_column")
            related = table.get("related")
            pk = table.get("pk")
            ai = table.get("ai")
            uk = table.get("uk")
            index = table.get("index")
            table = table["table"]

        if table not in self.schema["tables"]:
            self.schema["tables"][table] = {"columns": {}, "indexes": {}}
        self.schema["tables"][table]["columns"][column] = {}

        self.schema["tables"][table]["columns"][column]["datatype"] = datatype
        self.schema["tables"][table]["columns"][column]["datalen"] = datalen
        self.schema["tables"][table]["columns"][column]["datadec"] = datadec
        self.schema["tables"][table]["columns"][column]["to_table"] = to_table
        self.schema["tables"][table]["columns"][column]["to_column"] = to_column
        self.schema["tables"][table]["columns"][column]["related"] = related
        self.schema["tables"][table]["columns"][column]["pk"] = pk
        self.schema["tables"][table]["columns"][column]["ai"] = ai
        self.schema["tables"][table]["columns"][column]["uk"] = uk
        self.schema["tables"][table]["columns"][column]["index"] = index

    def add_index(self, table="", index_expression="", index_name=""):
        if table not in self.schema["indexes"]:
            self.schema["indexes"][table] = {}
        self.schema["indexes"][table]["expression"] = index_expression
        self.schema["indexes"][table]["name"] = index_name

    def get_schema_indexes(self):
        rez = []
        for x in self.schema["indexes"]:
            di = dict(self.schema["indexes"][x])
            di['table'] = x
            rez.append(di)
        return rez

    def get_schema_table_attr(self, table="", column="", attr=""):
        """
        returs schema data for given table, column, attribute
            get_schema_table_attr(table_name) - all columns
            get_schema_table_attr(table_name,column_name) - given column
            get_schema_table_attr(table_name,column_name,"datalen") - given attribute
        """
        rez = self.schema.get("tables", {})
        if table == "":
            return rez
        rez = rez.get(table, {}).get("columns", {})
        if column == "":
            return rez
        rez = rez.get(column, {})
        if attr == "":
            return rez
        return rez.get(attr, "")

    def get_schema_tables(self):
        return self.get_schema_table_attr()

    def get_schema_columns(self, table=""):
        return self.get_schema_table_attr(table)

    def get_schema_attr(self, table="", column=""):
        return self.get_schema_table_attr(table, column)

    def get_primary_tables(self, child_table, child_record):
        """
        returns list of foreign key tables and columns
        for given 'child_table' and 'child_record'

        used by Q2Db for integrity checking when INSERT/UPDATE
        """
        rez = []
        for child_column_name in self.get_schema_table_attr(child_table):
            child_column = self.get_schema_table_attr(child_table, child_column_name)
            if child_column.get("to_table") and child_column.get("to_column"):
                rez.append(
                    {
                        "primary_table": child_column.get("to_table"),
                        "primary_column": child_column.get("to_column"),
                        "child_column": child_column_name,
                        "child_value": child_record.get(child_column_name, ""),
                    }
                )
        return rez

    def get_child_tables(self, primary_table, primary_record):
        """
        returns list of foreign key tables and columns
        for given 'primary_table' and 'primary_record'

        used by Q2Db for integrity checking when DELETE
        """
        rez = []
        for linked_table_name in self.get_schema_table_attr():
            for linked_column_name in self.get_schema_table_attr(linked_table_name):
                linked_column = self.get_schema_table_attr(linked_table_name, linked_column_name)
                if linked_column.get("to_table") == primary_table and linked_column.get("to_column"):
                    parentCol = linked_column.get("to_column")
                    rez.append(
                        {
                            "child_table": linked_table_name,
                            "child_column": linked_column_name,
                            "parent_column": parentCol,
                            "parent_value": primary_record.get(parentCol),
                        }
                    )
        return rez

    @staticmethod
    def show_table(file, table="example_table"):
        if file.lower().endswith(".csv"):
            print(file)
            rows = [x for x in csv.DictReader(open(file), dialect="excel")]
        else:
            rows = json.load(open(file))

        schema = {}
        for row in rows:
            for col in row:
                if col not in schema:
                    schema[col] = {}
                schema[col]["lenght"] = max(schema[col].get("lenght", 0), len(row[col]))

        for x in schema:
            print(f"schema.add(table='{table}', '{x}', datatype='char', datalen={schema[x]['lenght']})")
