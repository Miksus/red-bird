import pytest
from datetime import date, datetime

from redbird.sql import insert, select, delete, update, count, execute, Table
from redbird.oper import in_, between


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


def test_getitem(engine):
    tbl = Table(name="getitems", engine=engine)
    tbl.create([
        {"name": "index_1", "type_": str, "primary_key": True},
        {"name": "index_2", "type_": int, "primary_key": True},
        {"name": "column_1", "type_": str, "primary_key": False},
        {"name": "column_2", "type_": int, "primary_key": False},
    ])
    tbl.insert([
        {"index_1": "a", "index_2": 1, "column_1": "Jack", "column_2": 10},
        {"index_1": "a", "index_2": 2, "column_1": "John", "column_2": 9},
        {"index_1": "b", "index_2": 1, "column_1": "James", "column_2": 8},
        {"index_1": "b", "index_2": 2, "column_1": "Johnny", "column_2": 7},
        {"index_1": "c", "index_2": 1, "column_1": "Jimmy", "column_2": 8},
        {"index_1": "c", "index_2": 2, "column_1": "Jim", "column_2": 7},
    ])

    assert tbl["a"] == [
        {"index_1": "a", "index_2": 1, "column_1": "Jack", "column_2": 10},
        {"index_1": "a", "index_2": 2, "column_1": "John", "column_2": 9},
    ]

    assert tbl[("a", 1)] == {"index_1": "a", "index_2": 1, "column_1": "Jack", "column_2": 10}
    assert tbl["a", 1] == {"index_1": "a", "index_2": 1, "column_1": "Jack", "column_2": 10}

    assert tbl[[("a", 1), ("b", 2)]] == [
        {"index_1": "a", "index_2": 1, "column_1": "Jack", "column_2": 10},
        {"index_1": "b", "index_2": 2, "column_1": "Johnny", "column_2": 7},
    ]
    assert tbl[["a", "b"]] == [
        {"index_1": "a", "index_2": 1, "column_1": "Jack", "column_2": 10},
        {"index_1": "a", "index_2": 2, "column_1": "John", "column_2": 9},
        {"index_1": "b", "index_2": 1, "column_1": "James", "column_2": 8},
        {"index_1": "b", "index_2": 2, "column_1": "Johnny", "column_2": 7},
    ]

    # Slicing
    assert tbl["a":"c"] == [
        {"index_1": "a", "index_2": 1, "column_1": "Jack", "column_2": 10},
        {"index_1": "a", "index_2": 2, "column_1": "John", "column_2": 9},
        {"index_1": "b", "index_2": 1, "column_1": "James", "column_2": 8},
        {"index_1": "b", "index_2": 2, "column_1": "Johnny", "column_2": 7},
        {"index_1": "c", "index_2": 1, "column_1": "Jimmy", "column_2": 8},
        {"index_1": "c", "index_2": 2, "column_1": "Jim", "column_2": 7},
    ]

    assert tbl["b":] == [
        {"index_1": "b", "index_2": 1, "column_1": "James", "column_2": 8},
        {"index_1": "b", "index_2": 2, "column_1": "Johnny", "column_2": 7},
        {"index_1": "c", "index_2": 1, "column_1": "Jimmy", "column_2": 8},
        {"index_1": "c", "index_2": 2, "column_1": "Jim", "column_2": 7},
    ]
    assert tbl[:"b"] == [
        {"index_1": "a", "index_2": 1, "column_1": "Jack", "column_2": 10},
        {"index_1": "a", "index_2": 2, "column_1": "John", "column_2": 9},
        {"index_1": "b", "index_2": 1, "column_1": "James", "column_2": 8},
        {"index_1": "b", "index_2": 2, "column_1": "Johnny", "column_2": 7},
    ]

def test_getitem_error(engine):
    tbl = Table(name="getitems", engine=engine)
    tbl.create([
        {"name": "index_1", "type_": str, "primary_key": True},
        {"name": "index_2", "type_": int, "primary_key": True},
        {"name": "column_1", "type_": str, "primary_key": False},
        {"name": "column_2", "type_": int, "primary_key": False},
    ])
    tbl.insert([
        {"index_1": "a", "index_2": 1, "column_1": "Jack", "column_2": 10},
        {"index_1": "a", "index_2": 2, "column_1": "John", "column_2": 9},
        {"index_1": "b", "index_2": 1, "column_1": "James", "column_2": 8},
        {"index_1": "b", "index_2": 2, "column_1": "Johnny", "column_2": 7},
        {"index_1": "c", "index_2": 1, "column_1": "Jimmy", "column_2": 8},
        {"index_1": "c", "index_2": 2, "column_1": "Jim", "column_2": 7},
    ])

    with pytest.raises(KeyError):
        # Not found, tuple (second key)
        tbl[("a", "not_found")]

    with pytest.raises(KeyError):
        # Not found, range
        tbl["not_found"]

    with pytest.raises(KeyError):
        # Not found, tuple (first key)
        tbl[("not_found", 2)]

    with pytest.raises(KeyError):
        # Not found, list of tuples
        tbl[[("not_found", 2)]]

    with pytest.raises(IndexError):
        # Too long for index
        tbl[("a", 2, "long")]

    with pytest.raises(IndexError):
        # Varying sizes
        tbl[[("a", 2), ("a",)]]

    with pytest.raises(IndexError):
        # Varying sizes
        tbl[[("a", 2), "a"]]

    with pytest.raises(TypeError):
        # Tuple in tuple
        tbl[("a", 2), "a"]

def test_getitem_no_primary_key(engine):
    tbl = Table(name="getitems", engine=engine)
    tbl.create([
        {"name": "index_1", "type_": str, "primary_key": False},
        {"name": "index_2", "type_": int, "primary_key": False},
        {"name": "column_1", "type_": str, "primary_key": False},
        {"name": "column_2", "type_": int, "primary_key": False},
    ])
    tbl.insert([
        {"index_1": "a", "index_2": 1, "column_1": "Jack", "column_2": 10},
        {"index_1": "a", "index_2": 2, "column_1": "John", "column_2": 9},
    ])
    with pytest.raises(TypeError):
        tbl[("a", 1)]