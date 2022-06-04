if __name__ == "__main__":
    import sys

    sys.path.insert(0, ".")


import os


from q2db.schema import Q2DbSchema
from q2db.db import Q2Db

import csv


def print_cursor(cursor):
    print(f"+--------- {cursor.table_name} === {cursor.row_count()} records, where = '{cursor.where}'")
    for x in cursor.records():
        print("|", x)
    print(f"+--end of- {cursor.table_name} ===")


def demo():

    csv_dict = csv.DictReader(open("temp/electronic-card-transactions-october-2021-csv-tables.csv"))

    schema = Q2DbSchema()
    if os.path.isfile("temp/csv.sqlite"):
        os.remove("temp/csv.sqlite")
    # db = Q2Db(database_name="temp/csv.sqlite", guest_mode=True)
    db = Q2Db(guest_mode=True)
    for x in csv_dict.fieldnames:
        schema.add(table="t1", column=x)

    db.set_schema(schema)

    for x in csv_dict:
        db.insert("t1", x)
    table = db.table("t1")
    # table = db.cursor(table_name="t1_LOG")
    print(table.row_count())
    print(table.record(0))


if __name__ == "__main__":

    demo()
