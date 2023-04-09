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


import sys

if __name__ == "__main__":

    sys.path.insert(0, ".")

    from demo.demo_mysql import demo
    # from demo.demo_postgresql import demo

    # from demo.demo_sqlite import demo

    demo()
    # from temp.try_pg_01 import demo

    # from tests.test_db import test_mysql, test_postgresql, test_sqlite
    # test_mysql()
    # test_sqlite()
    # test_postgresql()

import re
import sqlite3 as db_sqlite_connector

# import mysql.connector as db_mysql_connector
# import psycopg2 as db_postgresql_connector

from q2db.utils import int_, is_sub_list
from q2db.cursor import Q2MysqlCursor, Q2SqliteCursor, Q2PostgresqlCursor
from q2db.schema import Q2DbSchema


class Q2Db:
    def __init__(
        self,
        db_engine_name=None,
        user=None,
        password=None,
        host=None,
        database_name=None,
        port=None,
        guest_mode=False,
        url=None,
        get_admin_credential_callback=None,
        create_only=None,
        root_user=None,
        root_password=None
    ):
        """
        :param url: 'sqlite3|mysql|postgresql://username:password@host:port/database'
            or
        :param db_engine_name: 'sqlite3'|'mysql'|'postgresql', if None - 'sqlite3'
        :param user=''
        :param password=''
        :param host=''
        :param database_name='', if empty and db_engine_name == 'sqlite' - ':memory:'
        :param port:''
        :param guest_mode:, if empty - False
        :param get_admin_credential_callback: function that gets db name, host and port
            and returns (user, password)
        """
        self.url = url
        self.guest_mode = guest_mode
        if url is not None:
            self._parse_url()
        else:
            if db_engine_name is None:
                db_engine_name = "sqlite3"
                if database_name is None:
                    database_name = ":memory:"
            self.db_engine_name = db_engine_name
            if self.db_engine_name == "sqlite":
                self.db_engine_name = "sqlite3"
            self.user = user
            self.password = password
            self.host = host
            self.database_name = database_name
            self.port = int_(port)

        self.root_user = root_user
        self.root_password = root_password
        self.connection = None
        self.get_admin_credential_callback = get_admin_credential_callback
        if self.db_engine_name not in ["mysql", "sqlite3", "postgresql"]:
            raise Exception(f"Sorry, wrong DBAPI engine - {self.db_engine_name}")

        self.last_sql_error = ""
        self.last_sql = ""
        self.last_error_data = {}
        self.last_record = ""
        self.migrate_error_list = []

        self.db_schema = None
        self.connection = None
        self.ec = '"'

        if self.db_engine_name == "mysql":
            try:
                import mysql.connector as db_mysql_connector

                self.db_api_engine = db_mysql_connector
                self.db_cursor_class = Q2MysqlCursor
                self.ec = "`"
            except Exception:
                raise Exception(
                    "Sorry, can not import mysql.connector - use: pip install mysql-connector-python"
                )
        elif self.db_engine_name == "postgresql":
            try:
                import psycopg2 as db_postgresql_connector

                self.db_api_engine = db_postgresql_connector
                self.db_cursor_class = Q2PostgresqlCursor
            except Exception:
                raise Exception("Sorry, can not import psycopg2 - use: pip install psycopg2-binary")
        elif self.db_engine_name == "sqlite3":
            self.db_api_engine = db_sqlite_connector
            self.db_cursor_class = Q2SqliteCursor

        if create_only:
            self.create()
            return
        else:
            try:
                self._connect()
                return
            except Exception:
                pass  # Do nothing - give chance to screate database!

        if self.create():
            self._connect()

    def _connect(self):
        self.connection = self.connect(
            user=self.user,
            password=self.password,
            host=self.host,
            database_name=self.database_name,
            port=self.port,
        )
        self.set_schema(Q2DbSchema())

    def get_admin_credential_callback(self, database, dbengine, host, port):
        pass

    @staticmethod
    def get_system_database_name(db_engine_name):
        return {"mysql": "mysql", "postgresql": "postgres"}[db_engine_name]

    @staticmethod
    def get_default_admin_name(db_engine_name):
        return {"mysql": "root", "postgresql": "postgres"}.get(db_engine_name, None)

    def create(self):
        """
        Take chance to create database
        """
        admin_database_name = Q2Db.get_system_database_name(self.db_engine_name)
        admin_database_user = Q2Db.get_default_admin_name(self.db_engine_name)
        # admin_database_name = {"mysql": "mysql", "postgresql": "postgres"}[self.db_engine_name]
        # admin_database_user = {"mysql": "root", "postgresql": "postgres"}[self.db_engine_name]

        if self.get_admin_credential_callback:
            root_user, root_password = self.get_admin_credential_callback(
                self.database_name, self.db_engine_name, self.host, self.port, admin_database_user
            )
        else:
            root_user = self.root_user if self.root_user else self.user
            root_password = self.root_password if self.root_password else self.password
        self.connection = self.connect(
            user=root_user,
            password=root_password,
            host=self.host,
            database_name=admin_database_name,
            port=self.port,
        )
        if self.db_engine_name == "mysql":
            self._cursor(sql=f"CREATE USER IF NOT EXISTS '{self.user}' IDENTIFIED BY '{self.password}'")
            self._cursor(sql=f"CREATE DATABASE IF NOT EXISTS {self.database_name}")
            self._cursor(sql=f"GRANT ALL PRIVILEGES ON {self.database_name}.* TO '{self.user}'")

        elif self.db_engine_name == "postgresql":
            self._cursor(sql=f"CREATE USER {self.user} WITH PASSWORD  '{self.password}'")
            self._cursor(sql=f"CREATE DATABASE {self.database_name} WITH OWNER = {self.user}")
            self._cursor(sql=f"GRANT ALL PRIVILEGES ON DATABASE {self.database_name} TO {self.user}")
        self.connection.close()
        return True

    def connect(
        self,
        user=None,
        password=None,
        host=None,
        database_name=None,
        port=None,
    ):
        connection = None
        if self.db_engine_name == "mysql":
            connection = self.db_api_engine.connect(
                user=user,
                password=password,
                host=host if host else "localhost",
                port=port if port else 3306,
                database=database_name,
            )
            connection.autocommit = True
        elif self.db_engine_name == "postgresql":
            connection = self.db_api_engine.connect(
                user=user,
                password=password,
                host=host if host else "localhost",
                port=port if port else 5432,
                database=database_name,
            )
            connection.autocommit = True
        elif self.db_engine_name == "sqlite3":
            connection = self.db_api_engine.connect(
                database=self.database_name, isolation_level=None, check_same_thread=False
            )
        return connection

    def _parse_url(self):
        self.db_engine_name = self.url.split(":")[0]
        self.user = self.url.split("//")[1].split(":")[0]
        self.password = self.url.split("//")[1].split(":")[1].split("@")[0]
        self.host = self.url.split("@")[1].split(":")[0]
        self.port = self.url.split("@")[1].split(":")[1].split("/")[0]
        self.database_name = self.url.split("@")[1].split("/")[1]

    def close(self):
        self.connection.close()

    def get_primary_key_columns(self, table_name=""):
        """returns database primary columns for given table"""
        if self.db_schema is not None:
            cols = self.db_schema.get_schema_table_attr(table_name)
            rez = {}
            for x in cols:
                if cols[x].get("pk"):
                    cols[x]["name"] = x
                    rez[x] = cols[x]
            return rez

        if self.db_engine_name in ("mysql", "postgresql"):
            return self.get_database_columns(table_name, "column_key='PRI'")
        elif self.db_engine_name == "sqlite3":
            return self.get_database_columns(table_name, "pk=1")

    def get_database_columns(self, table_name="", filter="", query_database=None):
        """returns database columns for given table"""

        if table_name.upper().startswith("LOG_"):
            table_name = table_name[4:]

        if self.db_schema is not None and filter == "" and query_database is None:
            cols = self.db_schema.get_schema_table_attr(table_name)
            for x in cols:
                cols[x]["name"] = x
            return cols

        cols = {}
        sql = self.db_cursor_class.get_table_columns_sql(table_name, filter, self.database_name)
        for x in self.cursor(sql).records():
            if "name" in x:
                if "datalen" not in x:  # SQLITE
                    if "(" in x["datatype"]:
                        field_length = re.search(r"\((.*?)\)", x["datatype"]).group(1)
                        if "," not in field_length:
                            x["datalen"] = field_length
                            x["datadec"] = "0"
                        else:
                            x["datalen"] = field_length.split(",")[0]
                            x["datadec"] = field_length.split(",")[1]
                        x["datatype"] = x["datatype"].split("(")[0]
                    else:
                        x["datalen"] = "0"
                        x["datadec"] = "0"
                cols[x["name"]] = x
        return cols

    def get_tables(self, table_name=""):
        """Returns a list of tables names from database"""
        table_select_clause = f" and TABLE_NAME='{table_name}'" if table_name else ""
        sql = self.db_cursor_class.get_table_names_sql(table_select_clause, self.database_name)
        return [x["table_name"] for x in self.cursor(sql).records()]

    def set_schema(self, db_schema: Q2DbSchema):
        """assign and migrate schema to database"""
        self.db_schema = db_schema
        return self.migrate_schema()

    def migrate_schema(self):
        """
        creates (no alter) tables and columns in a physical database
        """
        if self.db_schema is None:
            return
        self.migrate_error_list = []
        _tables = [x for x in self.db_schema.get_schema_tables()]
        _tables += [x for x in self.get_tables() if x not in _tables]
        for table in _tables:
            # column that are already in
            database_columns = self.get_database_columns(table, query_database=True)
            schema_columns = self.db_schema.get_schema_columns(table)
            if not self.guest_mode:
                self._add_q2_columns(schema_columns)
            for column in schema_columns:
                colDic = self.db_schema.get_schema_attr(table, column)
                if column not in database_columns:  # new column
                    colDic["table"] = table
                    colDic["column"] = column
                    self.create_column(colDic)
                else:  # change schema as it is in database
                    colDic["datalen"] = database_columns[column]["datalen"]
                    colDic["datadec"] = database_columns[column]["datadec"]
                    colDic["ai"] = database_columns[column]["ai"]
                    colDic["pk"] = database_columns[column]["pk"]

            for column in database_columns:  # pull columns from db
                if column not in schema_columns:
                    database_columns[column]["table"] = table
                    self.db_schema.add(database_columns[column])
        self.migrate_indexes()
        return True

    def _add_q2_columns(self, schema_columns):
        """adding q2-columns to the q2-schema"""
        schema_columns["q2_time"] = {"datatype": "bigint"}
        schema_columns["q2_mode"] = {"datatype": "char", "datalen": 1}
        # schema_columns["update_time"] = {"datatype": "bigint"}
        # schema_columns["insert_session_id"] = {"datatype": "int"}
        # schema_columns["update_session_id"] = {"datatype": "int"}
        # schema_columns["user_lock"] = {"datatype": "char", "datalen": 1}
        pass

    def create_column(self, column_definition):
        """migrate given 'column_definition' to database"""

        column_definition["datadec"] = column_definition.get("datadec", 0)
        column_definition["primarykey"] = "PRIMARY KEY" if column_definition.get("pk", "") else ""
        if column_definition.get("to_table") and column_definition.get("to_column"):
            # pull attributes from primary table
            primary_column_definition = self.db_schema.get_schema_attr(
                column_definition["to_table"], column_definition["to_column"]
            )
            if primary_column_definition:
                for x in ["datatype", "datalen", "datadec"]:
                    column_definition[x] = primary_column_definition[x]

        if "datatype" not in column_definition:
            return None
        datatype = column_definition["datatype"].upper()
        if datatype[:3] in ["NUM", "DEC"]:
            column_definition["datatype"] = "NUMERIC"
            column_definition["default"] = "DEFAULT '0'"
            column_definition["size"] = "({datalen},{datadec})".format(**column_definition)
        elif "INT" in datatype:
            if self.db_engine_name == "sqlite3":
                column_definition["datatype"] = "INTEGER"
            column_definition["default"] = "DEFAULT '0'"
            column_definition["size"] = ""
        elif "CHAR" in datatype:
            column_definition["default"] = "DEFAULT ''"
            column_definition["size"] = (
                "({datalen})".format(**column_definition) if column_definition["datalen"] else ""
            )
        elif "DATE" in datatype:
            # column_definition["default"] = "DEFAULT '0000-00-00'"
            column_definition["default"] = ""
            column_definition["size"] = ""
        elif "TEXT" in datatype:
            column_definition["default"] = ""
            column_definition["size"] = ""
            if self.db_engine_name == "postgresql":
                column_definition["datatype"] = "TEXT"
        else:
            column_definition["size"] = ""
            column_definition["default"] = ""

        _column_definition = dict(column_definition)

        _column_definition["escape_char"] = self.ec

        _column_definition["autoincrement"] = ""
        if _column_definition.get("ai"):
            _column_definition["default"] = ""
            if self.db_engine_name == "mysql":
                _column_definition["autoincrement"] = "AUTO_INCREMENT"
            elif self.db_engine_name == "sqlite3":
                _column_definition["autoincrement"] = "AUTOINCREMENT"
            elif self.db_engine_name == "postgresql":
                _column_definition["datatype"] = ""
                _column_definition["primarykey"] = ""
                _column_definition["autoincrement"] = "SERIAL PRIMARY KEY"

        sql_column_text = """ {escape_char}{column}{escape_char}
                                {datatype} {size}
                                {primarykey}
                                {autoincrement}
                                {default}""".format(
            **_column_definition
        )
        table = column_definition["table"]

        if table in self.get_tables(table):
            sql_cmd = f"ALTER TABLE {self.ec}{table}{self.ec} ADD {sql_column_text}"
        else:
            sql_cmd = f"CREATE TABLE {self.ec}{table}{self.ec} ({sql_column_text})"
        if not self.run_migrate_sql(sql_cmd):
            return False

        if not self.guest_mode and not table.upper().startswith("LOG_"):
            self.create_index(column_definition)

            log_column_definition = dict(column_definition)
            log_column_definition["pk"] = ""
            log_column_definition["ai"] = ""
            log_column_definition["uk"] = ""
            log_column_definition["table"] = "log_" + log_column_definition["table"]
            self.create_column(log_column_definition)
        return True

    def create_index(self, column_definition):
        """
        create index for column
        """
        if (
            column_definition.get("index")
            or column_definition.get("column") == "name"
            or column_definition.get("column") == "date"
            or column_definition.get("to_table")
        ):
            column_definition["escape_char"] = self.ec
            sql_cmd = (
                "CREATE INDEX {escape_char}{table}_{column}{escape_char} ".format(**column_definition)
                + " on {escape_char}{table}{escape_char} ".format(**column_definition)
                + " ({escape_char}{column}{escape_char})".format(**column_definition)
            )

            self.run_migrate_sql(sql_cmd)

    def run_migrate_sql(self, sql_cmd):
        self._cursor(sql_cmd)
        # if 'Group' in sql_cmd:
        if self.last_sql_error != "":
            self.migrate_error_list.append(f"{self.last_sql_error}: {sql_cmd}")
            return False
        return True

    def migrate_indexes(self):
        if self.guest_mode:
            return
        indexes = self.db_schema.get_schema_indexes()
        for x in indexes:
            x["escape_char"] = self.ec
            if not x.get("name"):
                x["name"] = re.sub(r"[^\w\d]+", "_", x["expression"])
            sql_cmd = (
                "CREATE INDEX {escape_char}{table}_{name}{escape_char} "
                " on {escape_char}{table}{escape_char} ({expression})"
            ).format(**x)
            self.run_migrate_sql(sql_cmd)

    def _sqlite_patch(self, sql, record, table_columns):
        """Adapt sql statement for sqlite - convert str to int, replace placeholder character to ?"""
        for x in record:
            if "int" in table_columns.get(x, {}).get("type", ""):
                try:
                    record[x] = int_(record[x])
                except Exception:
                    print(table_columns.get(x, {}).get("type", ""), record[x])
        sql = sql.replace("%s", "?")
        return sql

    def _check_record_for_numbers(self, table, record):
        """ "makes sure that all number columns value is not blank string"""
        for x in record:
            if record[x] != "":
                continue
            datatype = self.db_schema.get_schema_attr(table, x).get("datatype")
            if datatype is None:
                continue
            if "int" in datatype or "dec" in datatype or "num" in datatype:
                record[x] = "0"

    def transaction(self):
        self._cursor(self.db_cursor_class._transaction)

    def commit(self):
        self._cursor("commit")

    def rollback(self):
        self._cursor("rollback")

    def raw_insert(self, table_name="", record={}):
        """insert dictionary into table"""
        if not (table_name or record):
            return None
        table_columns = self.get_database_columns(table_name[:])
        sql = (
            f"insert into {self.ec}{table_name}{self.ec} ("
            + ",".join([f"{self.ec}{x}{self.ec}" for x in record.keys() if x in table_columns])
            + ") values ("
            + ",".join(["%s" for x in record.keys() if x in table_columns])
            + ")"
        )

        if self.db_engine_name == "sqlite3":
            sql = sql.replace("%s", "?")

        data = [record[x] for x in record.keys() if x in table_columns]

        if not data:
            self.last_sql_error = f"no data to insert into table '{table_name}'"
            return False

        self._cursor(sql, data)

        if self.last_sql_error:
            return False
        else:
            return True

    def insert(self, table_name="", record={}):
        """
        insert dictionary into table
        """
        if not (table_name and record):
            return None

        if not table_name.upper().startswith("LOG_"):
            record["q2_time"] = f"{self.cursor().now()}"
            record["q2_mode"] = "i"
            # check foreign keys
            foreign_keys_list = self.db_schema.get_primary_tables(table_name, record)
            for x in foreign_keys_list:
                x["escape_char"] = self.ec
                if (
                    self.get(
                        x["primary_table"],
                        "{escape_char}{primary_column}{escape_char}= '{child_value}' ".format(**x),
                        x["child_value"],
                    )
                    == {}
                ):
                    self.last_sql_error = (
                        "Foreign key error for insert:"
                        + f" For {self.ec}{table_name}{self.ec}"
                        + ".{escape_char}{child_column}{escape_char}".format(**x)
                        + " not found value '{child_value}' ".format(**x)
                        + "in table "
                        + x["primary_table"]
                        + ".{primary_column}".format(**x)
                    )
                    self.last_error_data = x
                    return False

        table_columns = self.get_database_columns(table_name[:])
        primary_key_columns = self.get_primary_key_columns(table_name)

        aipk = ""
        if not table_name.upper().startswith("LOG_") and len(primary_key_columns) == 1:
            for d in primary_key_columns:
                if primary_key_columns[d].get("ai"):
                    aipk = d

        columns_list = [x for x in record if x in table_columns]
        if not table_name.upper().startswith("LOG_"):
            if not aipk:
                # create primary key value
                for x in primary_key_columns:
                    if x not in columns_list:
                        columns_list.append(x)
                    is_string_data = True if ("char" in primary_key_columns[x]["datatype"]) else False
                    if is_string_data:
                        primary_key_value = record.get(x, "")
                    else:
                        primary_key_value = int_(record.get(x, 0))
                    while (
                        self.cursor(
                            sql=f"""select {self.ec}{x}{self.ec}
                                    from {self.ec}{table_name}{self.ec}
                                    where {self.ec}{x}{self.ec}='{primary_key_value}'
                                """
                        ).row_count()
                        > 0
                    ):
                        if is_string_data:
                            primary_key_value += "."
                        else:
                            primary_key_value += 1
                    record[x] = primary_key_value
            else:  # autoincrement
                for pkname in primary_key_columns:
                    if pkname in record:
                        del record[pkname]
                        columns_list.pop(columns_list.index(pkname))

        sql = (
            f"insert into {self.ec}{table_name}{self.ec} ("
            + ",".join([f"{self.ec}{x}{self.ec}" for x in columns_list])
            + ") values ("
            + ",".join(["%s" for x in columns_list])
            + ")"
        )

        if self.db_engine_name == "sqlite3":
            sql = self._sqlite_patch(sql, record, table_columns)

        self._check_record_for_numbers(table_name, record)
        data = [record[x] for x in columns_list]

        if not data:
            self.last_sql_error = f"no data to insert into table '{table_name}'"
            return False

        self._cursor(sql, data)

        if self.last_sql_error:
            return False
        else:
            if aipk:
                if self.db_engine_name == "sqlite3":
                    record[aipk] = self._cursor("SELECT last_insert_rowid() as aipk")[0]["aipk"]
            if not table_name.upper().startswith("LOG_") and table_name.upper() != "PLATFORM":
                self.insert("log_" + table_name, record)
            return True

    def update(self, table_name="", record={}):
        """update from dictionary to table"""
        if not (table_name and record):
            return None

        table_columns = self.get_database_columns(table_name)
        primary_key_columns = self.get_primary_key_columns(table_name)

        if not table_name.upper().startswith("LOG_"):
            record["q2_time"] = f"{self.cursor().now()}"
            record["q2_mode"] = "u"
            foreign_keys_list = self.db_schema.get_primary_tables(table_name, record)
            for x in foreign_keys_list:
                if x["child_column"] not in record:  # column not going to change - skip checking
                    continue
                if self.get(x["primary_table"], "%(primary_column)s='%(child_value)s'" % x) == {}:
                    self.last_sql_error = (
                        "Foreign key error for update:"
                        + f" For {table_name}"
                        + ".{child_column}".format(**x)
                        + " not found value '{child_value}' ".format(**x)
                        + "in table "
                        + x["primary_table"]
                        + ".{primary_column}".format(**x)
                    )
                    self.last_error_data = x
                    return False

        columns_list = [x for x in record if x in table_columns]
        if is_sub_list(primary_key_columns.keys(), record.keys()):
            sql = f"update {self.ec}{table_name}{self.ec} set " + ",".join(
                [
                    f" {self.ec}{x}{self.ec}=%s "
                    for x in record
                    if x not in primary_key_columns and x in columns_list
                ]
            )
            sql += " where " + " and ".join([f" {self.ec}{x}{self.ec} = %s " for x in primary_key_columns])
            if self.db_engine_name == "sqlite3":
                sql = self._sqlite_patch(sql, record, table_columns)

            self._check_record_for_numbers(table_name, record)
            data = [record[x] for x in record if x not in primary_key_columns and x in columns_list]
            data += [record[x] for x in primary_key_columns]

            self._cursor(sql, data)

            if self.last_sql_error:
                return False
            else:
                if not table_name.upper().startswith("LOG_") and table_name.upper() != "PLATFORM":
                    self.insert("log_" + table_name, dict(record))
                return True
        else:
            self.last_sql_error = "Update requires a primary key column!"

    def delete(self, table_name="", record={}):
        if not (table_name and record):
            return None
        self.last_error_data = {}
        for x in self.db_schema.get_child_tables(table_name, record):
            x["escape_char"] = self.ec
            rez = self._cursor(
                """select 1 from {escape_char}{child_table}{escape_char}
                    where {escape_char}{child_column}{escape_char}='{parent_value}'""".format(
                    **x
                )
            )
            if {} != rez:
                self.last_sql_error = (
                    "Foreign key error for delete:"
                    + f" Row in {self.ec}{table_name}{self.ec}"
                    + ".{escape_char}{parent_column}{escape_char}".format(**x)
                    + "={parent_value}".format(**x)
                    + " can not to be deleted, because "
                    + ' it used in table "{child_table}"."{child_column}"'.format(**x)
                )
                self.last_error_data = x

                return False
        table_columns = self.get_database_columns(table_name)
        primary_key_columns = self.get_primary_key_columns(table_name)
        if set(self.get_primary_key_columns(table_name)).issubset(set(primary_key_columns.keys())):
            columns_list = [x for x in record if x in primary_key_columns]
        else:
            columns_list = [x for x in record if x in table_columns]
        where_clause = " and ".join([f"{self.ec}{x}{self.ec} = %s " for x in columns_list])
        data = [record[x] for x in columns_list]

        select_sql = f"select * from {self.ec}{table_name}{self.ec} where {where_clause}"
        if self.db_engine_name == "sqlite3":
            select_sql = self._sqlite_patch(select_sql, record, table_columns)

        row_to_be_deleted = self._cursor(select_sql, data)
        for x in row_to_be_deleted:
            row_to_be_deleted[x]["q2_mode"] = "d"
            row_to_be_deleted[x]["q2_time"] = f"{self.cursor().now()}"
            self.insert("log_" + table_name, row_to_be_deleted[x])

        sql = f"delete from {table_name} where {where_clause}"
        if self.db_engine_name == "sqlite3":
            sql = self._sqlite_patch(sql, record, table_columns)

        self._cursor(sql, data)

        if self.last_sql_error:
            return False
        else:
            return True

    def get(self, table_name="", where="", column_name=""):
        """returns value of given column or record dictionary
        from first row  given table_name for where condition
        """
        column_name = f"({column_name}) as ret " if column_name else "*"
        row = self._cursor(f"""select {column_name} from {table_name} where {where}""")
        if self.last_sql_error:
            return ""
        else:
            if row:
                if column_name == "*":
                    return row[0]
                else:
                    return row[0]["ret"]
        return {}

    def get_uniq_value(self, table_name, column, start_value):
        datatype = self.db_schema.get_schema_attr(table_name, column)["datatype"]
        if "int" in datatype or "dec" in datatype or "num" in datatype:
            return self.cursor(
                f"""select min({self.ec}{column}{self.ec}) +1 as pkvalue
                            from {self.ec}{table_name}{self.ec}
                            where {self.ec}{column}{self.ec} >= {start_value}
                                and {self.ec}{column}{self.ec} + 1 not in
                                    (select {self.ec}{column}{self.ec} from {self.ec}{table_name}{self.ec})
                        """
            ).r.pkvalue
        else:
            return start_value + "."

    def _dict_factory(self, cursor, row, sql):
        return {
            col[0]: f"{row[idx] if row[idx] is not None else ''}".rstrip()
            for idx, col in enumerate(cursor.description)
        }

    def _cursor(self, sql, data=[]):
        self.last_sql_error = ""
        self.last_sql = ""
        self.last_record = ""
        _rows = {}
        if self.db_engine_name == "postgresql":
            sql = sql.replace("`", '"')
        try:
            _work_cursor = self.connection.cursor()
            if data:
                _work_cursor.execute(sql, data)
            else:
                _work_cursor.execute(sql)
            if _work_cursor.description:
                i = 0
                for x in _work_cursor.fetchall():
                    _rows[i] = self._dict_factory(_work_cursor, x, sql)
                    i += 1
        except self.db_api_engine.Error as err:
            self.last_sql_error = str(err) + "> " + sql
            self.last_sql = sql
            self.last_record = "!".join([f"{x}" for x in data])
            _rows = {0: {}}
        return _rows

    def cursor(self, sql="", table_name="", order="", where="", data=[], cache_flag=True):
        # TODO - sanitaze where and order
        return self.db_cursor_class(
            self,
            sql,
            table_name=table_name,
            order=order,
            where=where,
            data=data,
            cache_flag=cache_flag,
        )

    def table(self, table_name="", order="", where="", cache_flag=True):
        return self.db_cursor_class(
            self,
            sql="",
            table_name=table_name,
            order=order,
            where=where,
            cache_flag=cache_flag,
        )
