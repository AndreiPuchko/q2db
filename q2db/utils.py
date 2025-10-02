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


def safe_identifier(name: str) -> str:
    """Return a safe SQL identifier or raise ValueError."""
    if not _VALID_SQL_IDENT.match(name.strip()):
        raise ValueError(f"Unsafe SQL identifier: {name!r}")
    return name


if __name__ == "__main__":
    # sql = "select coalesce( (select 99 from (select  1) tmp where not exists (select 1 from `topic_table` where `uid`=99) ), ( select min(`uid`) +1 as pkvalue from `topic_table`  where `uid` >= ( select max(`uid`) from `topic_table` where `uid`<=99 )  and `uid` + 1 not in (select `uid` from `topic_table`) )) as pkvalue"
    # sql = "select rownum  from  (  select z1.*, @i := @i + 1 as rownum  from ( select uid from `topic_table` order by uid ) z1, (select @i:= -1) z2  ) qq  where uid = '3'"
    # sql = """select name, type as datatype, `notnull` as nn , (SELECT "*" FROM sqlite_master WHERE tbl_name="topic_table" and ww.pk=1 and sql LIKE "%AUTOINCREMENT%") as ai  from PRAGMA_table_info("topic_table") ww"""
    # sql = """insert into "topic_table" ("name","q2_time","q2_mode","uid") values (1,2,3,4) """
    # sql = """insert into "topic_table" ("name","uid","q2_time","q2_mode") values (%s,%s,%s,%s) """
    sql = """sql  LIKE  "%AUTOINCREMENT%" """
    nsql, data = parse_sql(sql)
    print(sql)
    print(nsql)
    print(data)
