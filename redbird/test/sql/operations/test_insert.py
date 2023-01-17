import pytest
from datetime import date

from redbird.sql import insert, execute, select

def test_insert_dict(engine):
    sqlalchemy = pytest.importorskip("sqlalchemy")
    insert(
        {'id': 'a', 'name': 'Johnny', 'birth_date': date(2000, 1, 1), 'score': 100}, 
        table="empty", 
        bind=engine
    )
    assert [
        {'id': 'a', 'name': 'Johnny', 'birth_date': '2000-01-01', 'score': 100}, 
    ] == list(select("select * from empty", bind=engine))

def test_insert_list_dicts(engine):
    sqlalchemy = pytest.importorskip("sqlalchemy")
    insert(
        [
            {'id': 'a', 'name': 'Johnny', 'birth_date': date(2000, 1, 1), 'score': 100},
            {'id': 'b', 'name': 'James', 'birth_date': date(2020, 1, 1), 'score': 200},
        ], 
        table="empty", 
        bind=engine
    )
    assert [
        {'id': 'a', 'name': 'Johnny', 'birth_date': '2000-01-01', 'score': 100}, 
        {'id': 'b', 'name': 'James', 'birth_date': '2020-01-01', 'score': 200},
    ] == list(select("select * from empty", bind=engine))