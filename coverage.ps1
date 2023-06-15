coverage erase
coverage run -m pytest tests
# coverage run tests/test_db.py
# coverage report -m
coverage html
explorer .\htmlcov\index.html