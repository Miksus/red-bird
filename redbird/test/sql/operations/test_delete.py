import pytest
from datetime import date

from redbird.sql import delete, execute, select

def test_delete_all(engine):
    sqlalchemy = pytest.importorskip("sqlalchemy")
    assert delete(
        {}, 
        table="populated", 
        bind=engine
    ) == 3
    assert [] == list(select("select * from populated", bind=engine))

@pytest.mark.parametrize("how", [
    "dict", "expressions"
])
def test_delete(engine, how):
    sqlalchemy = pytest.importorskip("sqlalchemy")

    if how == "dict":
        where = {'name': 'Jack', 'birth_date': date(2000, 1, 1)}
    elif how == "expressions":
        where = (sqlalchemy.Column("name") == "Jack") & (sqlalchemy.Column("birth_date") == date(2000, 1, 1))

    assert delete(
        where, 
        table="populated", 
        bind=engine
    ) == 1
    assert [
        #{'id': 'a', 'name': 'Jack', 'birth_date': '2000-01-01', 'score': 100},
        {'id': 'b', 'name': 'John', 'birth_date': '1990-01-01', 'score': 200},
        {'id': 'c', 'name': 'James', 'birth_date': '2020-01-01', 'score': 300},
    ] == list(select("select * from populated", bind=engine))