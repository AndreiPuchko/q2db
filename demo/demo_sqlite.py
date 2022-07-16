if __name__ == "__main__":
    import sys

    sys.path.insert(0, ".")
    from demo import demo as demo_basic
else:
    from demo.demo import demo as demo_basic

from q2db.db import Q2Db


def demo():
    demo_basic(Q2Db("sqlite3", database_name=":memory:"))
    # demo_basic(Q2Db("sqlite3", database_name="temp/q2dbtest.sqlite"))


if __name__ == "__main__":
    demo()
