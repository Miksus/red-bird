import pytest
from datetime import date

from redbird.sql.expressions import select, Table

def test_enter_commit(engine):
    tbl = Table(bind=engine, name="empty")

    with tbl.transaction() as trans:
        trans.insert(
            {'id': 'a', 'name': 'Johnny', 'birth_date': date(2000, 1, 1), 'score': 100},
        )
        trans.insert(
            {'id': 'b', 'name': 'James', 'birth_date': date(2020, 1, 1), 'score': 100},
        )
    assert [
        {'id': 'a', 'name': 'Johnny', 'birth_date': "2000-01-01", 'score': 100},
        {'id': 'b', 'name': 'James', 'birth_date': "2020-01-01", 'score': 100},
    ] == list(select("select * from empty", bind=engine))

def test_enter_rollback(engine):
    tbl = Table(bind=engine, name="empty")
    try:
        with tbl.transaction() as trans:
            trans.insert(
                {'id': 'a', 'name': 'Johnny', 'birth_date': date(2000, 1, 1), 'score': 100},
            )
            trans.insert(
                {'id': 'b', 'name': 'James', 'birth_date': date(2020, 1, 1), 'score': 100},
            )
            raise RuntimeError("Oops")
    except RuntimeError:
        ...
    assert [] == list(select("select * from empty", bind=engine))

def test_enter_manual_rollback(engine):
    tbl = Table(bind=engine, name="empty")

    with tbl.transaction() as trans:
        trans.insert(
            {'id': 'a', 'name': 'Johnny', 'birth_date': date(2000, 1, 1), 'score': 100},
        )
        trans.insert(
            {'id': 'b', 'name': 'James', 'birth_date': date(2020, 1, 1), 'score': 100},
        )
        trans.rollback()

    assert [] == list(select("select * from empty", bind=engine))

def test_transaction_commit(engine):
    tbl = Table(bind=engine, name="empty")
    trans = tbl.open_transaction()
    trans.insert(
        {'id': 'a', 'name': 'Johnny', 'birth_date': date(2000, 1, 1), 'score': 100},
    )
    trans.insert(
        {'id': 'b', 'name': 'James', 'birth_date': date(2020, 1, 1), 'score': 100},
    )
    trans.commit()

    assert [
        {'id': 'a', 'name': 'Johnny', 'birth_date': "2000-01-01", 'score': 100},
        {'id': 'b', 'name': 'James', 'birth_date': "2020-01-01", 'score': 100},
    ] == list(select("select * from empty", bind=engine))

def test_transaction_rollback(engine):
    tbl = Table(bind=engine, name="empty")
    trans = tbl.open_transaction()
    trans.insert(
        {'id': 'a', 'name': 'Johnny', 'birth_date': date(2000, 1, 1), 'score': 100},
    )
    trans.insert(
        {'id': 'b', 'name': 'James', 'birth_date': date(2020, 1, 1), 'score': 100},
    )
    trans.rollback()

    assert [] == list(select("select * from empty", bind=engine))