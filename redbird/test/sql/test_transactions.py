import pytest
from datetime import date, datetime

import sqlalchemy
from redbird.sql.expressions import insert, select, delete, update, count, Table
from sqlalchemy import create_engine

def test_enter_commit(engine):
    tbl = Table(bind=engine, name="empty")

    with tbl as trans:
        trans.insert(
            {'id': 'a', 'name': 'Johnny', 'birth_date': date(2000, 1, 1), 'score': 100},
        )
        trans.insert(
            {'id': 'b', 'name': 'James', 'birth_date': date(2020, 1, 1), 'score': 100},
        )
    assert [
        {'id': 'a', 'name': 'Johnny', 'birth_date': "2000-01-01", 'score': 100},
        {'id': 'b', 'name': 'James', 'birth_date': "2020-01-01", 'score': 100},
    ] == list(engine.execute(sqlalchemy.text("select * from empty")).mappings())

def test_enter_rollback(engine):
    tbl = Table(bind=engine, name="empty")
    try:
        with tbl as trans:
            trans.insert(
                {'id': 'a', 'name': 'Johnny', 'birth_date': date(2000, 1, 1), 'score': 100},
            )
            trans.insert(
                {'id': 'b', 'name': 'James', 'birth_date': date(2020, 1, 1), 'score': 100},
            )
            raise RuntimeError("Oops")
    except RuntimeError:
        ...
    assert [] == list(engine.execute(sqlalchemy.text("select * from empty")).mappings())

def test_enter_manual_rollback(engine):
    tbl = Table(bind=engine, name="empty")

    with tbl as trans:
        trans.insert(
            {'id': 'a', 'name': 'Johnny', 'birth_date': date(2000, 1, 1), 'score': 100},
        )
        trans.insert(
            {'id': 'b', 'name': 'James', 'birth_date': date(2020, 1, 1), 'score': 100},
        )
        trans.rollback()

    assert [] == list(engine.execute(sqlalchemy.text("select * from empty")).mappings())

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
    ] == list(engine.execute(sqlalchemy.text("select * from empty")).mappings())

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

    assert [] == list(engine.execute(sqlalchemy.text("select * from empty")).mappings())