import pytest
from datetime import date, datetime

from redbird.sql import insert, select, delete, update, count, execute, Table

def test_select_all(engine):
    results = select(table="populated", engine=engine)
    assert [
        {'id': 'a', 'name': 'Jack', 'birth_date': date(2000, 1, 1), 'score': 100},
        {'id': 'b', 'name': 'John', 'birth_date': date(1990, 1, 1), 'score': 200},
        {'id': 'c', 'name': 'James', 'birth_date': date(2020, 1, 1), 'score': 300},
    ] == list(results)

def test_select_string(engine):
    results = select("SELECT * FROM populated", engine=engine)
    assert [
        {'id': 'a', 'name': 'Jack', 'birth_date': '2000-01-01', 'score': 100},
        {'id': 'b', 'name': 'John', 'birth_date': '1990-01-01', 'score': 200},
        {'id': 'c', 'name': 'James', 'birth_date': '2020-01-01', 'score': 300},
    ] == list(results)

def test_select_dict(engine):
    results = select({"name": "James", "birth_date": date(2020, 1, 1)}, table="populated", engine=engine)
    assert [
        #{'id': 'a', 'name': 'Jack', 'birth_date': '2000-01-01', 'score': 100},
        #{'id': 'b', 'name': 'John', 'birth_date': '1990-01-01', 'score': 200},
        {'id': 'c', 'name': 'James', 'birth_date': date(2020, 1, 1), 'score': 300},
    ] == list(results)

def test_select_expr(engine):
    sqlalchemy = pytest.importorskip("sqlalchemy")
    results = select((sqlalchemy.Column("name") == "James") & (sqlalchemy.Column("birth_date") == date(2020, 1, 1)), table="populated", engine=engine)
    assert [
        #{'id': 'a', 'name': 'Jack', 'birth_date': '2000-01-01', 'score': 100},
        #{'id': 'b', 'name': 'John', 'birth_date': '1990-01-01', 'score': 200},
        {'id': 'c', 'name': 'James', 'birth_date': date(2020, 1, 1), 'score': 300},
    ] == list(results)

def test_insert_dict(engine):
    sqlalchemy = pytest.importorskip("sqlalchemy")
    insert(
        {'id': 'a', 'name': 'Johnny', 'birth_date': date(2000, 1, 1), 'score': 100}, 
        table="empty", 
        engine=engine
    )
    assert [
        {'id': 'a', 'name': 'Johnny', 'birth_date': '2000-01-01', 'score': 100}, 
    ] == list(engine.execute(sqlalchemy.text("select * from empty")).mappings())

def test_delete(engine):
    sqlalchemy = pytest.importorskip("sqlalchemy")
    delete(
        {'name': 'Jack', 'birth_date': date(2000, 1, 1)}, 
        table="populated", 
        engine=engine
    )
    assert [
        #{'id': 'a', 'name': 'Jack', 'birth_date': '2000-01-01', 'score': 100},
        {'id': 'b', 'name': 'John', 'birth_date': '1990-01-01', 'score': 200},
        {'id': 'c', 'name': 'James', 'birth_date': '2020-01-01', 'score': 300},
    ] == list(engine.execute(sqlalchemy.text("select * from populated")).mappings())

def test_update(engine):
    sqlalchemy = pytest.importorskip("sqlalchemy")
    update(
        {'name': 'Jack', 'birth_date': date(2000, 1, 1)},
        {'score': 0},
        table="populated", 
        engine=engine
    )
    assert [
        {'id': 'a', 'name': 'Jack', 'birth_date': '2000-01-01', 'score': 0},
        {'id': 'b', 'name': 'John', 'birth_date': '1990-01-01', 'score': 200},
        {'id': 'c', 'name': 'James', 'birth_date': '2020-01-01', 'score': 300},
    ] == list(engine.execute(sqlalchemy.text("select * from populated")).mappings())

def test_count(engine):
    assert count(table="populated", engine=engine) == 3

@pytest.mark.parametrize("how", [
    "string", "expression"
])
def test_execute(engine, how):
    import sqlalchemy
    if how == "string":
        stmt = """
            INSERT INTO empty(id, name, score, birth_date) 
            VALUES ("a", "Max", 100, "2022-12-31")
        """
    elif how == "expression":
        stmt = sqlalchemy.text("""
            INSERT INTO empty(id, name, score, birth_date) 
            VALUES ("a", "Max", 100, "2022-12-31")
        """)
    execute(stmt, engine=engine)
    
    assert list(select(table="empty", engine=engine)) == [
        {
            "id": "a", "name": "Max", "score": 100, "birth_date": date(2022, 12, 31)
        }
    ]

def test_count_where(engine):
    assert count({"name": "John"}, table="populated", engine=engine) == 1
    assert count({"name": "Johnz"}, table="populated", engine=engine) == 0

