if __name__ == "__main__":
    import sys

    sys.path.insert(0, ".")

from q2db.utils import parse_sql


def test1():
    print(parse_sql("select w+'t' from q where d=123 and k=6'; DROP TABLE sites;'"))


if __name__ == "__main__":
    test1()
