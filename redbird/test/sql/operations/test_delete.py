import pytest
from datetime import date

from redbird.sql import delete, execute

def test_delete(engine):
    sqlalchemy = pytest.importorskip("sqlalchemy")
    assert delete(
        {'name': 'Jack', 'birth_date': date(2000, 1, 1)}, 
        table="populated", 
        engine=engine
    ) == 1
    assert [
        #{'id': 'a', 'name': 'Jack', 'birth_date': '2000-01-01', 'score': 100},
        {'id': 'b', 'name': 'John', 'birth_date': '1990-01-01', 'score': 200},
        {'id': 'c', 'name': 'James', 'birth_date': '2020-01-01', 'score': 300},
    ] == list(execute(sqlalchemy.text("select * from populated"), engine=engine).mappings())