if __name__ == "__main__":
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


class Record:
    def __init__(self, t):
        self.t = t

    def __getattr__(self, name):
        return self.t.record(self.t.current_row()).get(name, f"column name '{name}' not found")


class Q2Cursor:
    def __init__(self, q2_db, sql, table_name="", order="", where="", data=[], cache_flag=False):
        self.q2_db = q2_db
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
        self._currentRow = 0
        self.refresh()
        self.r = Record(self)

    def get_table_names_sql(table_select_clause="", database_name=""):
        """returns sql statement (depending database) for select a list of all tables or given table"""
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

    def lastDeleteData(self):
        return self.q2_db.lastDeleteData

    def subFilter(self, column, text):
        if self.table_name:
            f = "".join(self.primary_key_columns)
            f += f",{column}" if column not in self.primary_key_columns else ""
            sql = f"select {f} from {self.table_name} where "
            if self.where:
                sql += f" {self.where} and "
            sql += f" ({self.prepColumnSearch(column,text)}) "
            if self.order:
                sql += f" order by {self.order}"
            error, _rows = self.q2_db._cursor(f"""{sql}""")
            return _rows
        return {}

    def prepColumnSearch(self, column, searchText="", placeHolder="before"):
        rez = []
        _or = []

        def _orAppend(rez):
            _or.append(f"({' and '.join(rez)})") if rez else ""

        mode = "like"
        for x in re.split(r"(\+|\-|\*)", searchText):
            if x == "+":
                mode = "like"
            elif x == "-":
                mode = "not like"
            elif x == "*":
                _orAppend(rez)
                rez = []
                mode = "like"
            elif x:
                if placeHolder == "before":
                    rez.append(f" {column} {mode} '%{x}%' ")
                else:
                    rez.append(f" '%{x}%' {mode} {column} ")
        _orAppend(rez)
        return " or ".join(_or)

    def update(self, data, refresh=True, where=True):
        if self.table_name:
            rez = self.q2_db.update(self.table_name, data)
            if refresh and rez:
                self.refresh()
            return rez

    def raw_insert(self, data):
        if self.table_name:
            return self.q2_db.raw_insert(self.table_name, data)

    def insert(self, data, refresh=True, where=True):
        if self.table_name:
            if self.where and where:
                for x in re.split(" and ", self.where, flags=re.IGNORECASE):
                    if x.count("=") == 1 and " OR " not in x.upper():
                        data[x.split("=")[0].strip()] = eval(x.split("=")[1])
            rez = self.q2_db.insert(self.table_name, data)
            if refresh and rez:
                self.refresh()
            return rez

    def delete(self, data, refresh=True):
        if self.table_name:
            rez = self.q2_db.delete(self.table_name, data)
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
        return self._currentRow

    def first(self):
        self._currentRow = 0

    def last(self):
        self.set_current_row(self.row_count() - 1)

    def next(self):
        self.set_current_row(self.current_row() + 1)

    def prev(self):
        self.set_current_row(self.current_row() - 1)

    def set_current_row(self, current_row):
        if current_row >= 0:
            if current_row < self.row_count():
                self._currentRow = current_row
            else:
                self.last()
        else:
            self._currentRow = 0

    def eof(self):
        return self._currentRow == self._row_count - 1

    def bof(self):
        return self._currentRow == 0

    def getPkRow(self, dataDic):
        pkName = [x for x in self.primary_key_columns][0]
        pkValue = str(dataDic[pkName])
        for x in range(self.row_count()):
            if self.record(x)[pkName] == pkValue:
                return x

    def seek_row(self, data_dic):
        row_counter = 0
        for x in self.records():
            if [z for z in data_dic if z in x and data_dic[z] == x[z]]:
                return row_counter
            row_counter += 1
        return row_counter

    def get_primary_key_columns(self):
        return self.primary_key_columns[:]

    def refresh(self):
        self._currentRow = 0
        if self.table_name:
            self.primary_key_columns = [x for x in self.q2_db.get_primary_key_columns(self.table_name)]
            self.sql = f"select * from {self.table_name}"
            if self.where:
                self.sql += f" where {self.where}"
            if self.order:
                self.sql += f" order by {self.order}"
        self._rows = self.q2_db._cursor(f"""{self.sql}""", self.data)
        if self.q2_db.last_sql_error or self._rows == {}:
            self._row_count = -1
        else:
            self._row_count = len(self._rows)
        return self

    def row_count(self):
        return self._row_count

    def records(self):
        for x in range(self._row_count):
            self._currentRow = x
            yield self.record(x)

    def record(self, rowNumber, columns=[]):
        if rowNumber in self._rows:
            if columns:
                return {x: self._rows[rowNumber][x] for x in self._rows[rowNumber] if x in columns}
            else:
                return self._rows[rowNumber]
        else:
            return {}

    def get_columns(self):
        return [x for x in self.record(0)]

    def _prepare_export(self, file):
        rez = []
        for x in self.records():
            rez.append(x)
        if not hasattr(file, "write"):
            write_to = open(file, "w")
        else:
            write_to = file
        return write_to, rez

    def _prepare_import(self, file):
        if not hasattr(file, "read"):
            read_from = open(file)
        else:
            read_from = file
        return read_from

    def import_json(self, file):
        """read json from file or file-like object

        ;param file: str or file-like object
        """
        if self.table_name:
            read_from = self._prepare_import(file)
            rows = json.load(read_from)
            for x in rows:
                self.insert(x)

    def export_json(self, file):
        """write json into file or file-like object

        ;param file: str or file-like object
        """
        write_to, rez = self._prepare_export(file)
        if rez:
            json.dump(rez, write_to, indent=1)

    def export_csv(self, file):
        """write csv(excel dialect) into file or file-like object

        ;param file: str or file-like object
        """
        write_to, rez = self._prepare_export(file)
        if rez:
            csv_writer = csv.DictWriter(write_to, [x for x in rez[0]], dialect="excel")
            csv_writer.writeheader()
            for x in rez:
                csv_writer.writerow(x)

    def import_csv(self, file):
        """read csv from file or file-like object

        ;param file: str or file-like object
        """
        if self.table_name:
            read_from = self._prepare_import(file)
            rows = csv.DictReader(read_from, dialect="excel")
            for x in rows:
                self.insert(x)


class Q2SqliteCursor(Q2Cursor):
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
                        name,
                        type as datatype,
                        `notnull` as nn,
                        `dflt_value` as `default`
                    from PRAGMA_table_info("{table_name}")
                    {where_clause}
                    """


class Q2MysqlCursor(Q2Cursor):
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
                        column_name as name,
                        data_type as datatype,
                        column_type ,
                        case when character_maximum_length<>0
                                then character_maximum_length
                                else numeric_precision
                        end as datalen,
                        numeric_scale as datadec
                    FROM INFORMATION_SCHEMA.COLUMNS
                    WHERE table_name = '{table_name}' and
                    table_schema='{database_name}'
                    {where_clause}
                    """


class Q2PostgresqlCursor(Q2Cursor):
    @staticmethod
    def get_table_names_sql(table_select_clause="", database_name=""):
        return f"""SELECT table_name
                    FROM information_schema.columns
                    where table_catalog='{database_name}' and
                        table_schema='public' {"and" if table_select_clause else ""} {table_select_clause}
                     """

    @staticmethod
    def get_table_columns_sql(table_name="", where_clause="", database_name=""):
        if where_clause:
            where_clause = f" and {where_clause}"
        return f"""
                    select
                        isc.column_name as name,
                        isc.data_type as datatype,
                        case when isc.character_maximum_length<>0
                                then isc.character_maximum_length
                                else isc.numeric_precision
                        end as datalen,
                        isc.numeric_scale as datadec,
                        column_key

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
