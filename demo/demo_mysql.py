if __name__ == "__main__":
    import sys

    sys.path.insert(0, ".")
    from demo import demo as demo_basic
else:
    from demo.demo import demo as demo_basic

from q2db.db import Q2Db


def demo():
    demo_database = Q2Db(
        "mysql", database_name="q2test", host="localhost", port="3308", user="root", password="q2test"
    )
    demo_basic(demo_database)


if __name__ == "__main__":
    demo()
