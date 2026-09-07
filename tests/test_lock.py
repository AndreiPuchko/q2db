import pytest

from q2db.db import Q2Db


def _check_lock_with_two_connections(database_config):
    first_database = None
    second_database = None
    lock_name = "q2db-lock-test"

    try:
        first_database = Q2Db(**database_config)
        second_database = Q2Db(**database_config)
    except Exception as error:
        pytest.skip(f"Database is unavailable: {error}")

    try:
        assert first_database.lock(lock_name, timeout=0) is True
        assert second_database.lock(lock_name, timeout=0) is False

        assert first_database.unlock(lock_name) is True
        assert second_database.lock(lock_name, timeout=0) is True
        assert second_database.unlock(lock_name) is True
    finally:
        if first_database is not None:
            first_database.close()
        if second_database is not None:
            second_database.close()


def test_postgresql_lock_unlock_with_two_connections():
    _check_lock_with_two_connections(
        {
            "db_engine_name": "postgresql",
            "database_name": "q2test",
            "host": "localhost",
            "port": 6432,
            "user": "q2user",
            "password": "q2test",
        }
    )


def test_mysql_lock_unlock_with_two_connections():
    _check_lock_with_two_connections(
        {
            "db_engine_name": "mysql",
            "database_name": "q2test",
            "host": "localhost",
            "port": 3308,
            "user": "root",
            "password": "q2test",
        }
    )