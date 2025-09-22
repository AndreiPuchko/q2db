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
    (?P<op><=|>=|<>|!=|=|<|>|LIKE|IN|AND|OR)|
    (?P<str>'(?:[^']|''|\\')*')|
    (?P<num>\d+(\.\d+)?)|
    (?P<ident>[A-Za-z_][A-Za-z0-9_]*|\*)|
    (?P<paren>[()])|
    (?P<comma>,)
    """,
    re.IGNORECASE | re.VERBOSE,
)


def parse_where(where: str):
    params = []
    result_sql = []

    for match in TOKEN_PATTERN.finditer(where):
        kind = match.lastgroup
        value = match.group()

        if kind == "space":
            result_sql.append(" ")
        elif kind in ("op", "ident", "paren", "comma"):
            result_sql.append(value)
        elif kind == "str":
            # remove quotes, unescape, append as parameter
            unquoted = value[1:-1].replace("''", "'")
            params.append(unquoted)
            result_sql.append("%s")
        elif kind == "num":
            params.append(float(value) if "." in value else int(value))
            result_sql.append("%s")
        else:
            raise ValueError(f"Unexpected token: {value}")

    return "".join(result_sql), tuple(params)
