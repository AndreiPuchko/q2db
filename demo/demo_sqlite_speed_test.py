if __name__ == "__main__":
    import sys

    sys.path.insert(0, ".")

from q2db.schema import Q2DbSchema
from q2db.db import Q2Db
import time
# import cProfile


def demo(demo_database: Q2Db):
    if demo_database.connection is None:
        raise Exception("No database connection")

    schema = Q2DbSchema()
    demo_database.set_schema(schema)  # create tables in database

    t = time.time()
    # demo_database._cursor("begin transaction")
    demo_database.transaction()
    # for x in range(100000):
    #     demo_database.insert("topic_table", {"name": f"{x}" * 50})
    # print(demo_database.table("topic_table").row_count())
    print(demo_database.table("customers").row_count())
    demo_database.table("customers").import_csv("temp/3.csv")
    print(demo_database.table("customers").row_count())

    print(time.time() - t)

    # print(demo_database.get_tables() + demo_database.get_tables())

    # demo_database.commit()
    demo_database.rollback()


if __name__ == "__main__":
    # cProfile.run('demo(Q2Db("sqlite3", database_name="temp/a1.sqlite"))')
    demo(Q2Db("sqlite3", database_name="temp/a1.sqlite"))
