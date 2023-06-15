if __name__ == "__main__":  # pragma: no cover
    import sys
    if "." not in sys.path:
        sys.path.insert(0, ".")

from q2db.utils import int_
from q2db.utils import num
from q2db.utils import is_sub_list


def test_int_():
    assert int_(1) == 1
    assert int_(1.9000) == 1
    assert int_("1") == 1
    assert int_("1.0") == 1
    assert int_(" 1.0 ") == 1
    assert int_(" 1.0d ") == 0


def test_num():
    assert f"{num(1.1)}" == "1.1"
    assert f"{num('1.1')}" == "1.1"


def test_is_sub_list():
    assert is_sub_list([1, 2], [1, 2, 3]) is True
    assert is_sub_list([1, 2], [1, 3]) is False


if __name__ == "__main__":  # pragma: no cover
    test_int_()
    test_num()
    test_is_sub_list()