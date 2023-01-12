import pytest
from datetime import date, datetime

from redbird.sql import insert, select, delete, update, count, execute, Table
from redbird.oper import in_, between, equal

def test_select_all(engine):
    results = select(table="populated", bind=engine)
    assert [
        {'id': 'a', 'name': 'Jack', 'birth_date': date(2000, 1, 1), 'score': 100},
        {'id': 'b', 'name': 'John', 'birth_date': date(1990, 1, 1), 'score': 200},
        {'id': 'c', 'name': 'James', 'birth_date': date(2020, 1, 1), 'score': 300},
    ] == list(results)

@pytest.mark.parametrize("how", [
    "raw string",
    "expression"
])
def test_select_parametize(engine, how):
    sqlalchemy = pytest.importorskip("sqlalchemy")
    if how == "raw string":
        qry = "select * from populated where name = :name"
    elif how == "expression":
        qry = sqlalchemy.text("select * from populated where name = :name")
        
    results = select(qry, bind=engine, parameters={"name": "Jack"})
    assert [
        {'id': 'a', 'name': 'Jack', 'birth_date': '2000-01-01', 'score': 100},
        #{'id': 'b', 'name': 'John', 'birth_date': '1990-01-01', 'score': 200},
        #{'id': 'c', 'name': 'James', 'birth_date': '2020-01-01', 'score': 300},
    ] == list(results)

@pytest.mark.parametrize("how", [
    "raw string",
    'native',
    "expression",
])
def test_select_equal(engine, how):
    import sqlalchemy

    if how == "raw string":
        qry = "SELECT * FROM populated where name == 'John'"
    elif how == "native":
        qry = {"name": "John"}
    elif how == "expression":
        qry = sqlalchemy.Column("name") == "John"
    results = select(qry, bind=engine, table="populated")
    assert [
        #{'id': 'a', 'name': 'Jack', 'birth_date': date(2000, 1, 1), 'score': 100},
        {'id': 'b', 'name': 'John', 'birth_date': date(1990, 1, 1), 'score': 200},
        #{'id': 'c', 'name': 'James', 'birth_date': date(2020, 1, 1), 'score': 300},
    ] == list(results)

@pytest.mark.parametrize("how", [
    "raw string",
    "expression",
    "operation",
])
def test_select_in(engine, how):
    import sqlalchemy

    if how == "raw string":
        qry = "SELECT * FROM populated where name in ('Jack', 'John')"
    elif how == "expression":
        qry = sqlalchemy.Column("name").in_(["Jack", "John"])
    elif how == "operation":
        qry = {"name": in_(["Jack", "John"])}
    results = select(qry, bind=engine, table="populated")
    assert [
        {'id': 'a', 'name': 'Jack', 'birth_date': date(2000, 1, 1), 'score': 100},
        {'id': 'b', 'name': 'John', 'birth_date': date(1990, 1, 1), 'score': 200},
        #{'id': 'c', 'name': 'James', 'birth_date': date(2020, 1, 1), 'score': 300},
    ] == list(results)

@pytest.mark.parametrize("how", [
    "raw string",
    'native',
    "expression",
    "operation",
])
def test_select_range(engine, how):
    import sqlalchemy
    start = 100
    end = 220
    if how == "raw string":
        qry = f"SELECT * FROM populated where score between {start} and {end}"
    elif how == "native":
        qry = {"score": slice(start, end)}
    elif how == "expression":
        qry = sqlalchemy.Column("score").between(start, end)
    elif how == "operation":
        qry = {"score": between(start, end)}
    results = select(qry, bind=engine, table="populated")
    assert [
        {'id': 'a', 'name': 'Jack', 'birth_date': date(2000, 1, 1), 'score': 100},
        {'id': 'b', 'name': 'John', 'birth_date': date(1990, 1, 1), 'score': 200},
        #{'id': 'c', 'name': 'James', 'birth_date': date(2020, 1, 1), 'score': 300},
    ] == list(results)

@pytest.mark.parametrize("how", [
    "raw string", "statement"
])
def test_select_without_table(engine, how):
    import sqlalchemy
    if how == "raw string":
        qry = "SELECT * FROM populated where name in ('Jack', 'John')"
    elif how == "statement":
        tbl = Table("populated", bind=engine).object
        qry = sqlalchemy.select(tbl).where(tbl.columns['name'].in_(["Jack", "John"]))

    results = select(qry, bind=engine)
    
    if how == "statement":
        assert [
            {'id': 'a', 'name': 'Jack', 'birth_date': date(2000, 1, 1), 'score': 100},
            {'id': 'b', 'name': 'John', 'birth_date': date(1990, 1, 1), 'score': 200},
            #{'id': 'c', 'name': 'James', 'birth_date': '2020-01-01', 'score': 300},
        ] == list(results)
    else:
        assert [
            {'id': 'a', 'name': 'Jack', 'birth_date': '2000-01-01', 'score': 100},
            {'id': 'b', 'name': 'John', 'birth_date': '1990-01-01', 'score': 200},
            #{'id': 'c', 'name': 'James', 'birth_date': '2020-01-01', 'score': 300},
        ] == list(results)

def test_select_dict(engine):
    results = select({"name": "James", "birth_date": date(2020, 1, 1)}, table="populated", bind=engine)
    assert [
        #{'id': 'a', 'name': 'Jack', 'birth_date': '2000-01-01', 'score': 100},
        #{'id': 'b', 'name': 'John', 'birth_date': '1990-01-01', 'score': 200},
        {'id': 'c', 'name': 'James', 'birth_date': date(2020, 1, 1), 'score': 300},
    ] == list(results)

def test_select_expr(engine):
    sqlalchemy = pytest.importorskip("sqlalchemy")
    results = select((sqlalchemy.Column("name") == "James") & (sqlalchemy.Column("birth_date") == date(2020, 1, 1)), table="populated", bind=engine)
    assert [
        #{'id': 'a', 'name': 'Jack', 'birth_date': '2000-01-01', 'score': 100},
        #{'id': 'b', 'name': 'John', 'birth_date': '1990-01-01', 'score': 200},
        {'id': 'c', 'name': 'James', 'birth_date': date(2020, 1, 1), 'score': 300},
    ] == list(results)