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

if __name__ == "__main__":  # pragma: no cover
    import sys

    if "." not in sys.path:
        sys.path.insert(0, ".")

    # from demo.demo_mysql import demo
    from demo.demo_sqlite import demo

    demo()

# import uuid
import re
import json
import csv

from datetime import datetime
from q2db.utils import num, int_
import time
import logging


class lazy_rows(dict):
    def __init__(self, value, cursor):
        super().__init__(value)
        self.cursor: Q2Cursor = cursor
        self.pk = self.cursor.primary_key_columns[0]
        self.column_count = 1
        self.fetch_row(0)

    def fetch_row(self, row_number):
        if row_number not in self:
            return {}
        pk_value = super().__getitem__(row_number).get(self.pk)
        sql = (
            f"select * from {self.cursor.ec}{self.cursor.table_name}{self.cursor.ec} "
            f" where {self.cursor.ec}{self.pk}{self.cursor.ec} = '{pk_value}'"
        )
        row = self.cursor.q2_db._cursor(f"{sql}").get(0)
        if row is None:
            logging.error(sql)
            return {}
        super().__getitem__(row_number).update(row)
        if row_number == 0:
            self.column_count = len(row)
        return row

    def __getitem__(self, __key):
        row = super().__getitem__(__key)
        if len(row) < self.column_count:
            row = self.fetch_row(__key)
        return row


class Record:
    def __init__(self, t):
        self.t = t

    def __getattr__(self, name):
        return self.t.record(self.t.current_row()).get(name, f"column name '{name}' not found")


class Q2Cursor:
    def __init__(self, q2_db, sql, table_name="", order="", where="", data=[], cache_flag=False):
        self.q2_db = q2_db
        self.ec = self.q2_db.ec
        self.sql = sql
        self.table_name = table_name
        self.order = order
        self.primary_key_columns = []
        self.where = where
        self.data = data
        self.cache_flag = cache_flag
        self._rows = {}
        self._columns = []
        self._row_count = 0
        self._current_row = 0
        self._cursor = self.q2_db.connection.cursor()
        self.refresh()
        self.r = Record(self)
        self.tick_callback = None

    def transaction(self):
        self.q2_db.transaction()

    def commit(self):
        self.q2_db.commit()

    def rollback(self):
        self.q2_db.rollback()

    def get_table_names_sql(table_select_clause="", database_name=""):
        """returns sql statement (depends of database) for select a list of all tables or given table"""
        pass

    def get_table_columns_sql(table_name="", where_clause="", database_name=""):
        """return database depending sql statement for select a list of all tables or given table"""
        pass

    def now(self):
        return datetime.now().strftime("%Y%m%d%H%M%S")

    def set_order(self, sort=""):
        """set order when cursor base on table"""
        self.order = sort
        return self

    def set_where(self, where=""):
        """set where condition when cursor base on table"""
        self.where = where
        return self

    def last_sql_error(self):
        return self.q2_db.last_sql_error

    def last_sql(self):
        return self.q2_db.last_sql

    def last_record(self):
        return self.q2_db.last_record

    def sub_filter(self, column, text):
        if self.table_name:
            f = "".join(self.primary_key_columns)
            f += f",{column}" if column not in self.primary_key_columns else ""
            sql = f"select {f} from {self.ec}{self.table_name}{self.ec} where "
            if self.where:
                sql += f" {self.where} and "
            sql += f" ({self.prepare_column_search(column,text)}) "
            if self.order:
                sql += f" order by {self.order}"
            _rows = self.q2_db._cursor(f"""{sql}""", _cursor=self._cursor)
            return _rows
        return {}

    def prepare_column_search(self, column, searchText="", placeHolder="before"):
        rez = []
        _or = []

        def _or_append(rez):
            _or.append(f"({' and '.join(rez)})") if rez else ""

        mode = "like"
        for x in re.split(r"(\+|\-|\*)", searchText):
            if x == "+":
                mode = "like"
            elif x == "-":
                mode = "not like"
            elif x == "*":
                _or_append(rez)
                rez = []
                mode = "like"
            elif x:
                if placeHolder == "before":
                    rez.append(f" {column} {mode} '%{x}%' ")
                else:
                    rez.append(f" '%{x}%' {mode} {column} ")
        _or_append(rez)
        return " or ".join(_or)

    def raw_update(self, data, refresh=True, where=True):
        if self.table_name:
            rez = self.q2_db.raw_update(self.table_name, data, _cursor=self._cursor)
            if refresh and rez:
                self.refresh()
            return rez

    def update(self, data, refresh=True, where=True):
        if self.table_name:
            rez = self.q2_db.update(self.table_name, data, _cursor=self._cursor)
            if refresh and rez:
                self.refresh()
            return rez

    def raw_insert(self, data):
        if self.table_name:
            return self.q2_db.raw_insert(self.table_name, data, _cursor=self._cursor)

    def insert(self, data, refresh=True, where=True, log=True):
        if self.table_name:
            if self.where and where:
                for x in re.split(" and ", self.where, flags=re.IGNORECASE):
                    if x.count("=") == 1 and " OR " not in x.upper():
                        data[x.split("=")[0].strip()] = eval(x.split("=")[1])
            rez = self.q2_db.insert(self.table_name, data, _cursor=self._cursor, log=log)
            if refresh and rez:
                self.refresh()
            return rez

    def delete(self, data, refresh=True):
        if self.table_name:
            rez = self.q2_db.delete(self.table_name, data, _cursor=self._cursor)
            if rez is True and refresh:
                self.refresh()
            return rez

    def get(self, where="", column_name=""):
        """returns value of given column or record dictionary
        from first row  given table_name for where condition
        """
        if self.table_name:
            return self.q2_db.get(self.table_name, where, column_name)
        return {}

    def current_row(self):
        return self._current_row

    def first(self):
        self._current_row = 0

    def last(self):
        self.set_current_row(self.row_count() - 1)

    def next(self):
        self.set_current_row(self.current_row() + 1)

    def prev(self):
        self.set_current_row(self.current_row() - 1)

    def set_current_row(self, current_row):
        if current_row >= 0:
            if current_row < self.row_count():
                self._current_row = current_row
            else:
                self.last()
        else:
            self._current_row = 0

    def eof(self):
        return self._current_row == self._row_count - 1

    def bof(self):
        return self._current_row == 0

    def seek_primary_key_row(self, dataDic):
        """
        seek for row with primary kev == dataDic[pk]
        return row index (row number)
        """
        pk_name = [x for x in self.primary_key_columns][0]
        pk_value = str(dataDic[pk_name])
        t = time.time()
        for x in range(self.row_count()):
            if time.time() - t > 0.5:
                return 0
            if pk_name in self._rows[x]:
                if self._rows[x][pk_name] == pk_value:
                    return x
            else:
                if self.record(x)[pk_name] == pk_value:
                    return x

    def seek_row(self, data_dic):
        """
        seek for a row which containing data_dic
        """
        row_counter = 0
        for x in self.records():
            sk = [z for z in data_dic if z in x and data_dic[z] == x[z]]
            if len(sk) == len(data_dic):
                return row_counter
            row_counter += 1
        return row_counter

    def get_primary_key_columns(self):
        return self.primary_key_columns[:]

    def get_uniq_value(self, column, start_value):
        """
        returns next global (whole table w/o where) unique value for the column
        """
        return self.q2_db.get_uniq_value(
            self.table_name,
            column,
            start_value,
        )

    def get_next_sequence(self, column, start_value=0):
        """
        returns next local (for the current where clause) unique value for the column
        """
        if self.table_name:
            sql = f"""
            select min({column}+1) as seq
            from {self.ec}{self.table_name}{self.ec}
            where {column} >=
                (
                    select max({self.ec}{column}{self.ec})
                    from {self.ec}{self.table_name}{self.ec}
                    where {self.ec}{column}{self.ec}<={start_value}
                            {'and (' if self.where else '' } {self.where} {')' if self.where else '' }
                )
                and {column}+1 not in
                    (
                    select {column}
                    from {self.ec}{self.table_name}{self.ec}
                    {'where' if self.where else '' } {self.where}
                    )
                {'and' if self.where else '' } {self.where}
            """
            try:
                seq = num(self.q2_db.cursor(sql).record(0)["seq"])
            except Exception:
                seq = 1

            return seq + (0 if seq else 1)
        else:
            return 0

    def refresh(self):
        self._current_row = 0
        if self.table_name:
            self.primary_key_columns = [x for x in self.q2_db.get_primary_key_columns(self.table_name)]
            self.sql = f"select * from {self.ec}{self.table_name}{self.ec}"
            if self.where:
                self.sql += f" where {self.where}"
            if self.order:
                self.sql += f" order by {self.order}"

        if self.table_name and self.primary_key_columns:
            pk = self.primary_key_columns[0]
            self._rows = lazy_rows(
                self.q2_db._cursor(
                    f"{self.sql.replace(' * ', ' '+pk+ ' ')}", self.data, _cursor=self._cursor
                ),
                self,
            )
        else:
            self._rows = self.q2_db._cursor(f"""{self.sql}""", self.data, _cursor=self._cursor)

        if self.q2_db.last_sql_error or self._rows == {}:
            self._row_count = -1
        else:
            self._row_count = len(self._rows)
        return self

    def row_count(self):
        return self._row_count

    def records(self):
        for x in range(self._row_count):
            self._current_row = x
            yield self.record(x)

    def record(self, row_number=None, columns=[]):
        if row_number is None:
            row_number = self._current_row
        if row_number in self._rows:
            if columns:
                return {x: self._rows[row_number][x] for x in self._rows[row_number] if x in columns}
            else:
                return self._rows[row_number]
        else:
            return {}

    def refresh_record(self, row):
        if isinstance(self._rows, lazy_rows):
            if row in self._rows:
                self._rows.fetch_row(row)

    def get_record(self, row_number=None, columns=[]):
        return self.record(row_number, columns)

    def get_columns(self):
        return [x for x in self.record(0)]

    def _prepare_import(self, file):
        if not hasattr(file, "read"):
            read_from = open(file, encoding="utf-8")
        else:
            read_from = file
        return read_from

    def import_json(self, file, tick_callback=None):
        """read json from file or file-like object
        ;param file: str or file-like object
        """
        if self.table_name:
            read_from = self._prepare_import(file)
            rows = json.load(read_from)
            self.import_rows(rows, tick_callback)

    def import_csv(self, file, tick_callback=None):
        """read csv from file or file-like object
        ;param file: str or file-like object
        """
        if self.table_name:
            read_from = self._prepare_import(file)
            rows = csv.DictReader(read_from, dialect="excel", delimiter=";")
            self.import_rows(rows, tick_callback)

    def import_rows(self, rows, tick_callback=None):
        self.transaction()
        for x in rows:
            self.insert(x, refresh=False)
            tick_callback() if tick_callback else None
            if self.last_sql_error():
                self.rollback()
                raise Exception(f"Import error: {self.last_sql_error()}, {self.last_sql()}")
        self.commit()
        self.refresh()

    def _prepare_export(self, file, tick_callback=None):
        rez = []
        for x in self.records():
            rez.append(x)
            tick_callback() if tick_callback else None

        if not hasattr(file, "write"):
            write_to = open(file, "w", encoding="utf-8")
        else:
            write_to = file
        write_to, rez = self.before_export(write_to, rez)
        return write_to, rez

    def before_export(self, write_to, rez):
        return write_to, rez

    def export_json(self, file, tick_callback=None):
        """write json into file or file-like object
        ;param file: str or file-like object
        """
        write_to, rez = self._prepare_export(file, tick_callback)
        if rez:
            json.dump(rez, write_to, indent=1)

    def export_csv(self, file, tick_callback=None):
        """write csv(excel dialect) into file or file-like object
        ;param file: str or file-like object
        """
        write_to, rez = self._prepare_export(file, tick_callback)
        if rez:
            csv_writer = csv.DictWriter(
                write_to, [x for x in rez[0]], dialect="excel", lineterminator="\n", delimiter=";"
            )
            csv_writer.writeheader()
            for x in rez:
                csv_writer.writerow(x)


class Q2SqliteCursor(Q2Cursor):
    _transaction = "begin transaction"

    @staticmethod
    def get_table_names_sql(table_select_clause="", database_name=""):
        return f"""SELECT distinct tbl_name as table_name
                FROM sqlite_master
                WHERE  type = 'table' {table_select_clause} """

    @staticmethod
    def get_table_columns_sql(table_name="", where_clause="", database_name=""):
        if where_clause:
            where_clause = f" where {where_clause}"
        return f"""select
                        name
                        , type as datatype
                        , `notnull` as nn
                        , `dflt_value` as `default`
                        , case when pk = 1 then '*' else ' ' end as pk
                        , (SELECT "*"
                            FROM sqlite_master
                            WHERE tbl_name="{table_name}"
                                and ww.pk=1
                                and sql LIKE "%AUTOINCREMENT%"
                            ) as ai
                    from PRAGMA_table_info("{table_name}") ww
                    {where_clause}
                    """


class Q2MysqlCursor(Q2Cursor):
    _transaction = "start transaction"

    @staticmethod
    def get_table_names_sql(table_select_clause="", database_name=""):
        return f"""select distinct table_name as table_name
                   FROM INFORMATION_SCHEMA.TABLES
                   WHERE table_schema='{database_name}' and
                       TABLE_TYPE<>'VIEW' {table_select_clause}
                """

    @staticmethod
    def get_table_columns_sql(table_name="", where_clause="", database_name=""):
        if where_clause:
            where_clause = f" and {where_clause}"
        return f"""select
                        column_name as name
                        , data_type as datatype
                        , column_type
                        , case when character_maximum_length<>0
                                then character_maximum_length
                                else numeric_precision
                        end as datalen
                        , numeric_scale as datadec
                        , column_key
                        , case when column_key = 'PRI' then '*' else ' ' end as pk
                        , case when extra = 'auto_increment' then '*' else ' ' end as ai
                    FROM INFORMATION_SCHEMA.COLUMNS
                    WHERE table_name = '{table_name}' and
                    table_schema='{database_name}'
                    {where_clause}
                    """

    def seek_primary_key_row(self, dataDic):
        """
        seek for row with primary kev == dataDic[pk]
        return row index (row number)
        """
        pk_name = [x for x in self.primary_key_columns][0]
        pk_value = str(dataDic[pk_name])
        _sql = self.sql.replace("*", pk_name)
        row_number = self.q2_db._cursor(
            f"""
                           select rownum
                            from
                            (
                            select z1.*, @i := @i + 1 as rownum
                            from ( {_sql} ) z1, (select @i:= -1) z2
                            ) qq
                            where {pk_name} = '{pk_value}'
                           """
        )
        return int_(row_number[0]["rownum"])


class Q2PostgresqlCursor(Q2Cursor):
    _transaction = "start transaction"

    @staticmethod
    def get_table_names_sql(table_select_clause="", database_name=""):
        return f"""SELECT table_name
                    FROM information_schema.columns
                    where table_catalog='{database_name}' and
                        table_schema='public' {table_select_clause}
                     """

    @staticmethod
    def get_table_columns_sql(table_name="", where_clause="", database_name=""):
        if where_clause:
            where_clause = f" and {where_clause}"
        return f"""
                    select
                        isc.column_name as name
                        , isc.data_type as datatype
                        , case when isc.character_maximum_length<>0
                                then isc.character_maximum_length
                                else isc.numeric_precision
                        end as datalen
                        , isc.numeric_scale as datadec
                        , column_key
                        , case when column_key = 'PRI' then '*' else ' ' end as pk
                        , case when isc.column_default like 'nextval%' then '*' else ' ' end as ai

                    from information_schema.columns isc

                    left join (
                                select 'PRI' as column_key,
                                    is_ccu.column_name,
                                    is_ccu.table_name,
                                    is_ccu.constraint_catalog as table_catalog
                                from information_schema.constraint_column_usage is_ccu,
                                    information_schema.table_constraints  is_tc
                                where is_ccu.constraint_name=is_tc.constraint_name and
                                    is_ccu.constraint_catalog = is_tc.constraint_catalog and
                                    is_ccu.table_name = is_tc.table_name
                                ) ist
                    on
                        isc.table_catalog = ist.table_catalog and
                        isc.table_name = ist.table_name and
                        isc.column_name = ist.column_name

                    where
                        isc.table_catalog = '{database_name}' and
                        isc.table_schema = 'public' and
                        isc.table_name = '{table_name}'
                        {where_clause}

                    order by ordinal_position
                    """
