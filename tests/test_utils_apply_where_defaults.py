from q2db.utils import apply_where_defaults


def test_simple_string():
    data = {}
    apply_where_defaults(data, "status = 'ok'")
    assert data == {"status": "ok"}


def test_simple_int():
    data = {}
    apply_where_defaults(data, "count = 10")
    assert data == {"count": 10}


def test_simple_float():
    data = {}
    apply_where_defaults(data, "price = 12.5")
    assert data == {"price": 12.5}


def test_null():
    data = {}
    apply_where_defaults(data, "deleted = NULL")
    assert data == {}


def test_multiple_and():
    data = {}
    apply_where_defaults(data, "a = 1 and b = 'x'")
    assert data == {"a": 1, "b": "x"}


def test_ignore_gt_lt():
    data = {}
    apply_where_defaults(data, "date >= '2022-01-01' and date <= '2022-01-31'")
    assert data == {}


def test_ignore_in_subquery():
    data = {}
    apply_where_defaults(
        data,
        "order_id in (select order_id from order_lines where product_id = 3)",
    )
    assert data == {}


def test_mixed_conditions():
    data = {}
    apply_where_defaults(
        data,
        "type = 'A' and amount > 100 and id in (select id from t)",
    )
    assert data == {"type": "A"}


def test_or_ignored():
    data = {}
    apply_where_defaults(data, "a = 1 or b = 2")
    assert data == {}


def test_existing_data_overwritten():
    data = {"a": 5}
    apply_where_defaults(data, "a = 1")
    assert data == {"a": 1}
