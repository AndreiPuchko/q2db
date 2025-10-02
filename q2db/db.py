#    Copyright © 2021 Andrei Puchko
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

if __name__ == "__main__":  # pragma: no cover

    # sys.path.insert(0, ".")

    # from demo.demo_postgresql import demo
    # from demo.demo_postgresql import demo
    from demo.demo_sqlite import demo

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

from q2db.utils import int_, is_sub_list, num, parse_sql, escape_sql_string, safe_identifier
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
        root_password=None,
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
        :param create_only: False, used for
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
            self.user = user
            self.password = password
            self.host = host
            self.database_name = database_name
            self.port = int_(port)

        self.database_name = escape_sql_string(self.database_name)

        if self.db_engine_name == "sqlite":
            self.db_engine_name = "sqlite3"

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
        self.ph = "%s"

        if self.db_engine_name == "mysql":
            try:
                import mysql.connector as db_mysql_connector

                self.db_api_engine = db_mysql_connector
                self.db_cursor_class = Q2MysqlCursor
                self.ec = "`"
            except Exception:  # pragma: no cover
                raise Exception(
                    "Sorry, can not import mysql.connector - use: pip install mysql-connector-python"
                )
        elif self.db_engine_name == "postgresql":
            try:
                import psycopg2 as db_postgresql_connector

                self.db_api_engine = db_postgresql_connector
                self.db_cursor_class = Q2PostgresqlCursor
            except Exception:  # pragma: no cover
                raise Exception("Sorry, can not import psycopg2 - use: pip install psycopg2-binary")
        elif self.db_engine_name == "sqlite3":
            self.db_api_engine = db_sqlite_connector
            self.db_cursor_class = Q2SqliteCursor
            self.ph = "?"

        if create_only:
            self.create()
            return
        else:
            try:
                self._connect()
                return
            except Exception as e:
                print(f"{e}")
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

    def get_admin_credential_callback(self, database, dbengine, host, port):  # pragma: no cover
        pass

    @staticmethod
    def get_system_database_name(db_engine_name):
        return {"mysql": "mysql", "postgresql": "postgres"}.get(db_engine_name, None)

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
            if self.root_user and self.root_user != self.user:
                root_user = self.root_user
                root_password = self.root_password
            else:
                root_user = self.user
                root_password = self.password
            # root_user = self.root_user if self.root_user else self.user
            # root_password = self.root_password if self.root_password else self.password
        self.connection = self.connect(
            user=root_user,
            password=root_password,
            host=self.host,
            database_name=admin_database_name,
            port=self.port,
        )
        if self.db_engine_name == "mysql":
            version = self.connection.get_server_version()
            if (version[0] * 10 + version[1]) > 55 or "MariaDB" in self.connection.get_server_info():
                self._cursor("CREATE USER IF NOT EXISTS %s IDENTIFIED BY %s", (self.user, self.password))
            else:
                if self._cursor("SELECT user FROM mysql.user where user =%s", (self.user,)) == {}:
                    self._cursor("CREATE USER %s IDENTIFIED BY %s", (self.user, self.password))
            self.raise_sql_error()
            self._cursor(sql=f"CREATE DATABASE IF NOT EXISTS {escape_sql_string(self.database_name)}")
            self.raise_sql_error()
            self._cursor(
                sql=f"GRANT ALL PRIVILEGES ON {escape_sql_string(self.database_name)}.* TO '{self.user}'"
            )
            self.raise_sql_error()
        elif self.db_engine_name == "postgresql":
            if self._cursor(" SELECT * FROM pg_catalog.pg_roles WHERE rolname = %s", (self.user,)) == {}:
                self._cursor("CREATE USER %s WITH PASSWORD  %s", (self.user, self.password))
                self.raise_sql_error()
            if (
                self._cursor(
                    " SELECT * FROM pg_catalog.pg_database  WHERE datname  = %s", (self.database_name,)
                )
                == {}
            ):
                self._cursor(sql=f"CREATE DATABASE {self.database_name} WITH OWNER = {self.user}")
                self.raise_sql_error()
            self._cursor(sql=f"GRANT ALL PRIVILEGES ON DATABASE {self.database_name} TO {self.user}")
            self.raise_sql_error()
        self.connection.close()
        return True

    def raise_sql_error(self):
        if self.last_sql_error:
            raise Exception(self.last_sql_error)

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

            def concat(*arg):
                return "".join(f"{x}" for x in arg)

            connection.create_function("concat", -1, concat)
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

    def ensure_empty_pk(self, table_name="", record={}):
        if not (table_name):
            return False
        pk = self.get_primary_key_columns(table_name)
        if pk:
            key = list(pk.keys())[0]
            if pk[key].get("ai"):
                return
            if pk[key].get("datatype") in ["char"]:
                record[key] = ""
            else:
                record[key] = "0"
            if "name" not in record:
                record["name"] = "-"
            if self.get(table_name, f"{key} = '{record[key]}'") == {}:
                self.insert(table_name, record)

    def ensure_record(self, table_name="", where="", record={}):
        if not (table_name and record):
            return False
        table_name = safe_identifier(table_name)
        row = self._cursor(f"""select * from `{table_name}` where {where}""")
        if row == {}:
            self.insert(table_name, record)
        else:
            record.update(row[0])
        return record

    def print_last_sql_error(self):
        if self.last_sql_error:
            print(self.last_sql_error)

    def get_database_columns(self, table_name="", filter="", query_database=None):
        """returns database columns for given table"""
        if not (table_name):
            return False
        table_name = safe_identifier(table_name)
        table_name = escape_sql_string(table_name)
        if table_name.upper().startswith("LOG_"):
            table_name = table_name[4:]

        if self.db_schema is not None and filter == "" and query_database is None:
            cols = self.db_schema.get_schema_table_attr(table_name)
            for x in cols:
                cols[x]["name"] = x
            return cols

        cols = {}
        sql = self.db_cursor_class.get_table_columns_sql(table_name, filter, self.database_name)
        # for x in self.cursor(sql).records():
        for x in [rec for rec in self._cursor(sql).values()]:
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
        if table_name:
            table_name = safe_identifier(table_name)
        table_select_clause = f" and TABLE_NAME='{escape_sql_string(table_name)}'" if table_name else ""
        sql = self.db_cursor_class.get_table_names_sql(table_select_clause, self.database_name)
        return [x["table_name"] for x in self._cursor(sql).values()]

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
        db_only_tables = [x for x in self.get_tables() if x not in _tables]
        _tables += db_only_tables
        for table_name in _tables:
            # column that are already in
            # if table == 'sqlite_sequence':
            #     continue
            database_columns = self.get_database_columns(table_name, query_database=True)
            schema_columns = self.db_schema.get_schema_columns(table_name)
            if not self.guest_mode and table_name not in db_only_tables:
                self._add_q2_columns(schema_columns)
            for column in schema_columns:
                column = safe_identifier(column)
                colDic = self.db_schema.get_schema_attr(table_name, column)
                if column not in database_columns:  # new column
                    colDic["table"] = table_name
                    colDic["column"] = column
                    self.create_column(colDic)
                else:  # change schema as it is in database
                    colDic["datalen"] = database_columns[column]["datalen"]
                    colDic["datadec"] = database_columns[column]["datadec"]
                    colDic["ai"] = database_columns[column]["ai"]
                    colDic["pk"] = database_columns[column]["pk"]

            for column in database_columns:  # pull columns from db
                if column not in schema_columns:
                    database_columns[column]["table"] = table_name
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

    def column_definition(self, column_definition):
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

        if "datatype" not in column_definition or column_definition.get("datatype") is None:
            return None
        column_definition["size"] = ""
        column_definition["default"] = ""

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
        return sql_column_text

    def create_column(self, column_definition):
        """migrate given 'column_definition' to database"""
        sql_column_text = self.column_definition(column_definition)
        if sql_column_text is None:
            return False
        table_name = column_definition["table"]
        table_name = safe_identifier(table_name)

        if table_name in self.get_tables(table_name):
            sql_cmd = f"ALTER TABLE {self.ec}{table_name}{self.ec} ADD {sql_column_text}"
        else:
            sql_cmd = f"CREATE TABLE {self.ec}{table_name}{self.ec} ({sql_column_text})"
        if not self.run_migrate_sql(sql_cmd):
            return False

        if not self.guest_mode and not table_name.upper().startswith("LOG_"):
            self.create_index(column_definition)

            log_column_definition = dict(column_definition)
            log_column_definition["pk"] = ""
            log_column_definition["ai"] = ""
            log_column_definition["uk"] = ""
            log_column_definition["table"] = "log_" + log_column_definition["table"]
            self.create_column(log_column_definition)
        return True

    def alter_column(self, column_definition):
        sql_column_text = self.column_definition(column_definition)
        table_name = column_definition["table"]
        table_name = safe_identifier(table_name)
        column = column_definition["column"]
        column = safe_identifier(column)
        self.migrate_error_list = []
        sql_cmd = (
            f"ALTER TABLE {self.ec}{table_name}{self.ec} "
            f"change column {self.ec}{column}{self.ec} {sql_column_text}"
        )
        if not self.run_migrate_sql(sql_cmd):
            return False
        self.migrate_schema()
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
        self._cursor(sql_cmd, safe=False)
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
            datatype = table_columns.get(x, {}).get("datatype", "").lower()
            if "int" == datatype[:3]:
                record[x] = int_(record[x])
            elif "dec" in datatype or "num" in datatype:
                record[x] = f"{record[x]}"
        return sql

    def _check_record_for_numbers(self, table_name, record):
        """ "makes sure that all number columns value is not blank string"""
        for x in record:
            if record[x] != "":
                continue
            datatype = self.db_schema.get_schema_attr(table_name, x).get("datatype", "").lower()
            if "int" in datatype or "dec" in datatype or "num" in datatype:
                record[x] = "0"

    def transaction(self):
        self._cursor(self.db_cursor_class._transaction)

    def commit(self):
        self._cursor("commit")

    def rollback(self):
        self._cursor("rollback")

    def raw_insert(self, table_name="", record={}, _cursor=None):
        """insert dicti or list of dict into table"""
        if table_name == "":
            return False
        table_name = safe_identifier(table_name)
        table_columns = self.get_database_columns(table_name[:])
        if isinstance(record, dict):
            columns_list = [x for x in record if x in table_columns]
        elif isinstance(record, list):
            columns_list = [x for x in record[0] if x in table_columns]
        else:
            self.last_sql_error = f"Wrong data to insert into table '{table_name}'"
            return False

        if not columns_list:
            self.last_sql_error = f"no data to insert into table '{table_name}'"
            return False

        sql = (
            f"insert into {self.ec}{table_name}{self.ec} ("
            + ",".join([f"{self.ec}{x}{self.ec}" for x in columns_list])
            + ") values ("
            + ",".join(["%s" for x in columns_list])
            + ")"
        )

        # if self.db_engine_name == "sqlite3":
        #     sql = sql.replace("%s", "?")

        if isinstance(record, dict):
            data = [record[x] for x in columns_list]
            self._cursor(sql, data, _cursor)
        if isinstance(record, list):
            for row in record:
                data = [row[x] for x in columns_list]
                self._cursor(sql, data, _cursor)

        if self.last_sql_error:
            return False
        else:
            return True

    def insert(self, table_name="", record={}, _cursor=None, log=True):
        """
        insert dictionary into table
        """
        if not (table_name and record):
            return False
        table_name = safe_identifier(table_name)

        if _cursor is None:
            _cursor = self.raw_cursor()

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
        if len(primary_key_columns) == 1:
            for d in primary_key_columns:
                if primary_key_columns[d].get("ai"):
                    aipk = d

        columns_list = [x for x in record if x in table_columns]
        if not aipk:
            # create primary key value
            self.make_pk(table_name, record, primary_key_columns, columns_list, _cursor)
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

        self._cursor(sql, data, _cursor)

        if self.last_sql_error:
            return False
        else:
            if aipk:
                if self.db_engine_name == "sqlite3":
                    record[aipk] = self._cursor("SELECT last_insert_rowid() as aipk")[0]["aipk"]
                elif self.db_engine_name == "mysql":
                    record[aipk] = self._cursor("SELECT last_insert_id() as aipk")[0]["aipk"]
                elif self.db_engine_name == "postgresql":
                    record[aipk] = self._cursor("SELECT LASTVAL() as aipk")[0]["aipk"]
            if log and not table_name.upper().startswith("LOG_") and table_name.upper() != "PLATFORM":
                self.raw_insert("log_" + table_name, record, _cursor)
            return True

    def make_pk(self, table_name, record, primary_key_columns, columns_list, _cursor):
        for pk in primary_key_columns:
            if pk not in columns_list:
                columns_list.append(pk)
            is_string_data = True if ("char" in primary_key_columns[pk]["datatype"]) else False
            if is_string_data:
                primary_key_value = record.get(pk, "")
            else:
                primary_key_value = int_(record.get(pk, 0))
            if not is_string_data:
                primary_key_value = self.get_uniq_value(table_name, pk, primary_key_value, _cursor)
            else:
                sql = f"""select {self.ec}{pk}{self.ec} as pk
                                        from {self.ec}{table_name}{self.ec}
                                        where {self.ec}{pk}{self.ec}='{{}}'
                                    """
                while True:
                    rez = self._cursor(sql.format(primary_key_value), _cursor=_cursor)
                    if rez == {}:
                        break
                    if is_string_data:
                        primary_key_value = str(primary_key_value) + "."
                    else:
                        primary_key_value += 1
            record[pk] = primary_key_value

    def raw_update(self, table_name="", record={}, _cursor=None):
        """update from dictionary to table"""
        if not (table_name and record):
            return False
        table_name = safe_identifier(table_name)

        if _cursor is None:
            _cursor = self.raw_cursor()

        table_columns = self.get_database_columns(table_name)
        primary_key_columns = self.get_primary_key_columns(table_name)

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

            self._cursor(sql, data, _cursor)

            if self.last_sql_error:
                return False
            else:
                if not table_name.upper().startswith("LOG_") and table_name.upper() != "PLATFORM":
                    self.raw_insert("log_" + table_name, dict(record), _cursor)
                return True
        else:
            self.last_sql_error = "Update requires a primary key column!"

    def update(self, table_name="", record={}, _cursor=None):
        """update from dictionary to table"""
        if not (table_name and record):
            return False
        table_name = safe_identifier(table_name)

        if _cursor is None:
            _cursor = self.raw_cursor()

        table_columns = self.get_database_columns(table_name)
        primary_key_columns = self.get_primary_key_columns(table_name)

        if not table_name.upper().startswith("LOG_"):
            record["q2_time"] = f"{self.cursor().now()}"
            record["q2_mode"] = "u"
            foreign_keys_list = self.db_schema.get_primary_tables(table_name, record)
            for x in foreign_keys_list:
                if x["child_column"] not in record:  # column not going to change - skip checking
                    continue
                x["child_column"] = safe_identifier(x["child_column"])
                x["primary_table"] = safe_identifier(x["primary_table"])
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

            self._cursor(sql, data, _cursor)

            if self.last_sql_error:
                return False
            else:
                if not table_name.upper().startswith("LOG_") and table_name.upper() != "PLATFORM":
                    self.raw_insert("log_" + table_name, dict(record), _cursor)
                return True
        else:
            self.last_sql_error = "Update requires a primary key column!"

    def delete(self, table_name="", record={}, _cursor=None):
        if not (table_name and record):
            return False
        table_name = safe_identifier(table_name)

        if _cursor is None:
            _cursor = self.raw_cursor()

        self.last_error_data = {}
        for x in self.db_schema.get_child_tables(table_name, record):
            x["escape_char"] = self.ec
            x["place_holder"] = "%s"
            x["child_table"] = safe_identifier(x["child_table"])
            x["child_column"] = safe_identifier(x["child_column"])
            sql = """select 1 from {escape_char}{child_table}{escape_char}
                    where {escape_char}{child_column}{escape_char}={place_holder}""".format(
                **x
            )
            rez = self._cursor(sql, (x["parent_value"],))
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
        if set(self.get_primary_key_columns(table_name)).issubset(set(record.keys())):
            columns_list = [x for x in record if x in primary_key_columns]
        else:
            columns_list = [x for x in record if x in table_columns]
        # columns_list = [x for x in record if x in table_columns]

        if columns_list == []:
            self.last_sql_error = f"No columns from table in data to delete: {record}"
            return False

        where_clause = " and ".join([f"{self.ec}{x}{self.ec} = %s " for x in columns_list])
        data = [record[x] for x in columns_list]

        select_sql = f"select * from {self.ec}{table_name}{self.ec} where {where_clause}"
        if self.db_engine_name == "sqlite3":
            select_sql = self._sqlite_patch(select_sql, record, table_columns)

        row_to_be_deleted = self._cursor(select_sql, data, _cursor)
        if row_to_be_deleted == {}:
            return False

        for x in row_to_be_deleted:
            row_to_be_deleted[x]["q2_mode"] = "d"
            row_to_be_deleted[x]["q2_time"] = f"{self.cursor().now()}"
            self.raw_insert("log_" + table_name, row_to_be_deleted[x], _cursor)

        sql = f"delete from {self.ec}{table_name}{self.ec} where {where_clause}"
        if self.db_engine_name == "sqlite3":
            sql = self._sqlite_patch(sql, record, table_columns)

        self._cursor(sql, data, _cursor)

        if self.last_sql_error:  # pragma: no cover
            return False
        else:
            return True

    def get(self, table_name="", where="", column_name=""):
        """returns value of given column or record dictionary
        from first row  given table_name for where condition
        """
        if not (table_name):
            return False
        if not column_name:
            column_name = "*"
        elif "," not in column_name:
            column_name = f"({column_name}) as ret "
        table_name = safe_identifier(table_name)
        if isinstance(where, str):
            where, data = parse_sql(where)
        elif isinstance(where, (list, tuple)):
            data = where[1]
            where = where[0]
            if not isinstance(data, (list, tuple)):
                data = [data]
        parsed_column_name, column_data = parse_sql(column_name)
        if column_data:
            data = column_data + data
            column_name = parsed_column_name
        # if self.db_engine_name == "sqlite3":
        #     where = where.replace("%s", "?")
        row = self._cursor(f"""select {column_name} from `{table_name}` where {where}""", data=data)
        if self.last_sql_error:
            return ""
        else:
            if row:
                if column_name == "*" or "," in column_name:
                    return row[0]
                else:
                    return row[0]["ret"]
        return {}

    def get_uniq_value(self, table_name, column, start_value, _cursor=None):
        table_name = safe_identifier(table_name)
        column = safe_identifier(column)
        datatype = self.db_schema.get_schema_attr(table_name, column).get("datatype")
        if datatype is None:
            return False
        datatype = datatype.lower()
        if "int" in datatype or "dec" in datatype or "num" in datatype:
            start_value = num(start_value)
            sql = f"""select coalesce(
                            (select {start_value} from (select 1) tmp where not exists
                                (select 1 from {self.ec}{table_name}{self.ec} where {self.ec}{column}{self.ec}={start_value})
                            ),
                            (
                            select min({self.ec}{column}{self.ec}) +1 as pkvalue
                            from {self.ec}{table_name}{self.ec}
                            where {self.ec}{column}{self.ec} >=
                                        (
                                            select max({self.ec}{column}{self.ec})
                                            from {self.ec}{table_name}{self.ec}
                                            where {self.ec}{column}{self.ec}<={start_value}
                                        )
                                and {self.ec}{column}{self.ec} + 1 not in
                                    (select {self.ec}{column}{self.ec} from {self.ec}{table_name}{self.ec})
                        )) as pkvalue
                        """
            dig = int_ if "int" in datatype else num
            return dig(self._cursor(sql, _cursor=_cursor).get(0, {}).get("pkvalue"))
        else:
            _pkvalue_list = re.split(r"([^\d]+)", start_value)
            _base = "".join(_pkvalue_list[:-1])
            _suffix = num(_pkvalue_list[-1]) + 1
            value = f"{_base}{_suffix}"
            return value

    def _dict_factory(self, cursor, row, sql):
        return {
            col[0]: f"{row[idx] if row[idx] is not None else ''}".rstrip()
            for idx, col in enumerate(cursor.description)
        }

    def raw_cursor(self):
        return self.connection.cursor()

    def parse_sql(self, sql, data=[]):
        return parse_sql(sql, data, placeholder=self.ph)

    def _cursor(self, sql, data=[], _cursor=None, safe=True):
        self.last_sql_error = ""
        self.last_sql = ""
        self.last_record = ""
        _rows = {}
        if self.db_engine_name == "postgresql":
            sql = sql.replace("`", '"')
        elif self.db_engine_name == "sqlite3":
            sql = sql.replace("%s", "?")
        try:
            if _cursor is None:
                # _cursor = self.connection.cursor()
                _cursor = self.raw_cursor()
            if data:
                _cursor.execute(sql, data)
            else:
                _cursor.execute(sql)
            if _cursor.description:
                i = 0
                for x in _cursor.fetchall():
                    _rows[i] = self._dict_factory(_cursor, x, sql)
                    i += 1
        except self.db_api_engine.Error as err:
            self.last_sql_error = str(err) + "> " + sql
            self.last_sql = sql
            self.last_record = "!".join([f"{x}" for x in data])
            # _rows = {0: {}}
            _rows = dict()
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
