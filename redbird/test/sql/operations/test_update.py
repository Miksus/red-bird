import pytest
from datetime import date, datetime

from redbird.sql import insert, select, delete, update, count, execute, Table
from redbird.oper import in_, between

def test_update_all(engine):
    sqlalchemy = pytest.importorskip("sqlalchemy")

    assert update(
        {},
        {'score': -999},
        table="populated", 
        bind=engine
    ) == 3

    assert [
        {'id': 'a', 'name': 'Jack', 'birth_date': '2000-01-01', 'score': -999},
        {'id': 'b', 'name': 'John', 'birth_date': '1990-01-01', 'score': -999},
        {'id': 'c', 'name': 'James', 'birth_date': '2020-01-01', 'score': -999},
    ] == list(select("select * from populated", bind=engine))

@pytest.mark.parametrize("how", [
    "dict", "expressions"
])
def test_update(engine, how):
    sqlalchemy = pytest.importorskip("sqlalchemy")

    if how == "dict":
        where = {'name': 'Jack', 'birth_date': date(2000, 1, 1)}
    elif how == "expressions":
        where = (sqlalchemy.Column("name") == "Jack") & (sqlalchemy.Column("birth_date") == date(2000, 1, 1))

    assert update(
        where,
        {'score': 0},
        table="populated", 
        bind=engine
    ) == 1
    assert [
        {'id': 'a', 'name': 'Jack', 'birth_date': '2000-01-01', 'score': 0},
        {'id': 'b', 'name': 'John', 'birth_date': '1990-01-01', 'score': 200},
        {'id': 'c', 'name': 'James', 'birth_date': '2020-01-01', 'score': 300},
    ] == list(select("select * from populated", bind=engine))