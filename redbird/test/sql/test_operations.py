import pytest
from datetime import date, datetime

from redbird.sql import insert, select, delete, update, count, execute, Table
from redbird.oper import in_, between


def test_count(engine):
    assert count(table="populated", bind=engine) == 3

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
    execute(stmt, bind=engine)
    
    assert list(select(table="empty", bind=engine)) == [
        {
            "id": "a", "name": "Max", "score": 100, "birth_date": date(2022, 12, 31)
        }
    ]

def test_count_where(engine):
    assert count({"name": "John"}, table="populated", bind=engine) == 1
    assert count({"name": "Johnz"}, table="populated", bind=engine) == 0


