#    Copyright (C) 2021 Andrei Puchko
#
#    Licensed under the Apache License, Version 2.0 (the "License");
#    you may not use this file except in compliance with the License.
#    You may obtain a copy of the License at
#
#        http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS,
#    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#    See the License for the specific language governing permissions and
#    limitations under the License.

from decimal import Decimal
import re
from typing import List, Optional, Tuple
import ast


def is_sub_list(sublst, lst):
    return len([x for x in sublst if x in lst]) == len(sublst)


def int_(toInt):
    try:
        return int(f"{toInt}")
    except Exception:
        return int(num(toInt))


def num(tonum):
    try:
        return Decimal(f"{tonum}")
    except Exception:
        return 0


TOKEN_PATTERN = re.compile(
    r"""
    (?P<space>\s+)|
    (?P<str>'(?:\\.|''|[^'])*')|
    (?P<ph>%s)|
    # LIKE patterns without quotes (e.g. %TEXT%)
    (?P<likepat>%[A-Za-z0-9_]+%?)|
    (?P<num>\d+(\.\d+)?)|
    # operators: comparisons + arithmetic
    (?P<op><=|>=|<>|!=|=|<|>|\+|-|\*|\.\*|/|%|LIKE|INSERT|INTO|IN|AND|OR|DEFAULT|AS|DISTINCT|:=|@)|
    # backtick identifiers (MySQL)
    (?P<bident>`[^`]+`)|
    # regular identifiers, with dot support
    (?P<ident>[A-Za-z_][A-Za-z0-9_]*(?:\.[A-Za-z_][A-Za-z0-9_]*)*|\*)|
    (?P<paren>[()])|
    (?P<comma>,)
    """,
    re.IGNORECASE | re.VERBOSE,
)


# TOKEN_PATTERN = re.compile(
#     r"""
#     (?P<space>\s+)|
#     # string literal: single or double quotes
#     (?P<str>'(?:\\.|''|[^'])*'|"(?:\\.|""|[^"])*")|
#     (?P<ph>%s)|
#     (?P<num>\d+(\.\d+)?)|
#     # operators: comparisons + arithmetic
#     (?P<op><=|>=|<>|!=|=|<|>|\+|-|\*|\.\*|/|%|LIKE|IN|AND|OR|DEFAULT|AS|DISTINCT|:=|@)|
#     # LIKE patterns without quotes (e.g. %TEXT%)
#     (?P<likepat>%[A-Za-z0-9_]+%?)|
#     # backtick identifiers (MySQL)
#     (?P<bident>`[^`]+`)|
#     # identifiers (with dot or *)
#     (?P<ident>[A-Za-z_][A-Za-z0-9_]*(?:\.[A-Za-z_][A-Za-z0-9_]*)*|\*)|
#     (?P<paren>[()])|
#     (?P<comma>,)
#     """,
#     re.IGNORECASE | re.VERBOSE,
# )


def _unquote_sql_string(s: str) -> str:
    inner = s[1:-1]
    inner = inner.replace("''", "'")
    inner = re.sub(r"\\(.)", r"\1", inner)
    return inner


def parse_sql(
    sql: str,
    datalist: Optional[List] = None,
    placeholder: Optional[str] = None,
) -> Tuple[str, Tuple]:
    if datalist is None:
        datalist = []

    params: List = []
    datalist_iter = iter(datalist)
    result_sql: List[str] = []

    last_keyword = None  # track last keyword (for DEFAULT detection)
    # i=0
    for match in TOKEN_PATTERN.finditer(sql):
        kind = match.lastgroup
        value = match.group()

        if kind == "space":
            result_sql.append(" ")
        elif kind in ("op", "ident", "paren", "comma", "bident"):
            result_sql.append(value)
            if value.upper() == "DEFAULT":
                last_keyword = "DEFAULT"
            else:
                last_keyword = None
        elif kind == "likepat":
            if value != "%s":
                result_sql.append('"')
            result_sql.append(value)  # keep %AUTOINCREMENT% intact
            if value != "%s":
                result_sql.append('"')
        elif kind == "str":
            if last_keyword == "DEFAULT":
                # keep as-is (don’t bind as parameter)
                result_sql.append(value)
                last_keyword = None
            else:
                unquoted = _unquote_sql_string(value)
                params.append(unquoted)
                result_sql.append(placeholder or "%s")

        elif kind == "num":
            if last_keyword == "DEFAULT":
                # keep numeric literal as-is
                result_sql.append(value)
                last_keyword = None
            else:
                params.append(float(value) if "." in value else int(value))
                result_sql.append(placeholder or "%s")

        elif kind == "ph":
            try:
                params.append(next(datalist_iter))
            except StopIteration:
                raise ValueError("Not enough values in datalist for %s placeholders")
            result_sql.append(placeholder or "%s")
            last_keyword = None

        else:
            raise ValueError(f"Unexpected token: {value!r}")

    try:
        extra = next(datalist_iter)
        raise ValueError(f"Too many values in datalist, unused value: {extra!r}")
    except StopIteration:
        pass

    return "".join(result_sql), tuple(params)


def escape_sql_string(s):
    """Escape special characters in a SQL string."""
    if isinstance(s, str):
        return s.replace("\\", "\\\\\\\\").replace("'", "\\'").replace('"', '\\"')
    else:
        return s


_VALID_SQL_IDENT = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def parse_where(where=""):
    if isinstance(where, str):
        where, data = parse_sql(where)
    elif isinstance(where, (list, tuple)):
        if len(where) > 1:
            data = where[1]
            if not isinstance(data, (list, tuple)):
                data = (data,)
        else:
            data = tuple()
        where = where[0]
    return where, data


def safe_identifier(name: str) -> str:
    """Return a safe SQL identifier or raise ValueError."""
    if not _VALID_SQL_IDENT.match(name.strip()):
        raise ValueError(f"Unsafe SQL identifier: {name!r}")
    return name


def apply_where_defaults(data: dict, where: str) -> None:
    """
    Forcibly applies to data only simple conditions of the form:
        field = literal

    Ignores everything else, including:
        - IN (...)
        - SELECT / subqueries
        - OR
        - >, <, >=, <=
        - expressions and functions

    Modifies data in place.
    """

    if not where:
        return

    simple_eq_re = re.compile(
        r"""
        ^\s*
        (?P<field>[a-zA-Z_][a-zA-Z0-9_]*)
        \s*=\s*
        (?P<value>
            '[^']*' |
            "[^"]*" |
            \d+(\.\d+)? |
            NULL
        )
        \s*$
        """,
        re.VERBOSE | re.IGNORECASE,
    )

    def parse_literal(value: str):
        value = value.strip()
        if value.upper() == "NULL":
            return None
        try:
            return ast.literal_eval(value)
        except Exception:
            return None

    for clause in re.split(r"\s+and\s+", where, flags=re.IGNORECASE):
        upper = clause.upper()

        # жёсткий фильтр сложных условий
        if " OR " in upper or " IN " in upper or " SELECT " in upper or ">" in clause or "<" in clause:
            continue

        m = simple_eq_re.match(clause)
        if not m:
            continue

        field = m.group("field")
        value = parse_literal(m.group("value"))

        if value is not None:
            data[field] = value
