import pytest
from datetime import date, datetime

from redbird.sql import insert, select, delete, update, count, execute, Table
from redbird.oper import in_, between

@pytest.fixture()
def table_single(engine) -> Table:
    tbl = Table(name="getitems_singleindex", engine=engine)
    tbl.create([
        {"name": "index_1", "type_": str, "primary_key": True},
        {"name": "column_1", "type_": str, "primary_key": False},
        {"name": "column_2", "type_": int, "primary_key": False},
    ])
    tbl.insert([
        {"index_1": "a", "column_1": "Jack", "column_2": 10},
        {"index_1": "b", "column_1": "James", "column_2": 8},
        {"index_1": "c", "column_1": "Jimmy", "column_2": 8},
    ])
    return tbl

@pytest.fixture()
def table(engine) -> Table:
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
    return tbl

def test_multi(table, table_single):
    # Single index
    assert table_single[["a", "b"]] == [
        {"index_1": "a", "column_1": "Jack", "column_2": 10},
        {"index_1": "b", "column_1": "James", "column_2": 8},
    ]

    # Multi indices
    assert table["a"] == [
        {"index_1": "a", "index_2": 1, "column_1": "Jack", "column_2": 10},
        {"index_1": "a", "index_2": 2, "column_1": "John", "column_2": 9},
    ]

    assert table[[("a", 1), ("b", 2)]] == [
        {"index_1": "a", "index_2": 1, "column_1": "Jack", "column_2": 10},
        {"index_1": "b", "index_2": 2, "column_1": "Johnny", "column_2": 7},
    ]
    assert table[["a", "b"]] == [
        {"index_1": "a", "index_2": 1, "column_1": "Jack", "column_2": 10},
        {"index_1": "a", "index_2": 2, "column_1": "John", "column_2": 9},
        {"index_1": "b", "index_2": 1, "column_1": "James", "column_2": 8},
        {"index_1": "b", "index_2": 2, "column_1": "Johnny", "column_2": 7},
    ]

def test_single_item(table, table_single):
    # Single index
    assert table_single["a"] == {"index_1": "a", "column_1": "Jack", "column_2": 10}

    # Multiple indices
    assert table[("a", 1)] == {"index_1": "a", "index_2": 1, "column_1": "Jack", "column_2": 10}
    assert table["a", 1] == {"index_1": "a", "index_2": 1, "column_1": "Jack", "column_2": 10}

def test_slice(table):
    assert table[:] == [
        {"index_1": "a", "index_2": 1, "column_1": "Jack", "column_2": 10},
        {"index_1": "a", "index_2": 2, "column_1": "John", "column_2": 9},
        {"index_1": "b", "index_2": 1, "column_1": "James", "column_2": 8},
        {"index_1": "b", "index_2": 2, "column_1": "Johnny", "column_2": 7},
        {"index_1": "c", "index_2": 1, "column_1": "Jimmy", "column_2": 8},
        {"index_1": "c", "index_2": 2, "column_1": "Jim", "column_2": 7},
    ]

    assert table["a":"c"] == [
        {"index_1": "a", "index_2": 1, "column_1": "Jack", "column_2": 10},
        {"index_1": "a", "index_2": 2, "column_1": "John", "column_2": 9},
        {"index_1": "b", "index_2": 1, "column_1": "James", "column_2": 8},
        {"index_1": "b", "index_2": 2, "column_1": "Johnny", "column_2": 7},
        {"index_1": "c", "index_2": 1, "column_1": "Jimmy", "column_2": 8},
        {"index_1": "c", "index_2": 2, "column_1": "Jim", "column_2": 7},
    ]

    assert table["b":] == [
        {"index_1": "b", "index_2": 1, "column_1": "James", "column_2": 8},
        {"index_1": "b", "index_2": 2, "column_1": "Johnny", "column_2": 7},
        {"index_1": "c", "index_2": 1, "column_1": "Jimmy", "column_2": 8},
        {"index_1": "c", "index_2": 2, "column_1": "Jim", "column_2": 7},
    ]
    assert table[:"b"] == [
        {"index_1": "a", "index_2": 1, "column_1": "Jack", "column_2": 10},
        {"index_1": "a", "index_2": 2, "column_1": "John", "column_2": 9},
        {"index_1": "b", "index_2": 1, "column_1": "James", "column_2": 8},
        {"index_1": "b", "index_2": 2, "column_1": "Johnny", "column_2": 7},
    ]

def test_getitem_error(table):

    with pytest.raises(KeyError):
        # Not found, tuple (second key)
        table[("a", "not_found")]

    with pytest.raises(KeyError):
        # Not found, range
        table["not_found"]

    with pytest.raises(KeyError):
        # Not found, tuple (first key)
        table[("not_found", 2)]

    with pytest.raises(KeyError):
        # Not found, list of tuples
        table[[("not_found", 2)]]

    with pytest.raises(IndexError):
        # Too long for index
        table[("a", 2, "long")]

    with pytest.raises(IndexError):
        # Varying sizes
        table[[("a", 2), ("a",)]]

    with pytest.raises(IndexError):
        # Varying sizes
        table[[("a", 2), "a"]]

    with pytest.raises(TypeError):
        # Tuple in tuple
        table[("a", 2), "a"]

def test_getitem_no_primary_key(table):
    with pytest.raises(TypeError):
        table[("a", 1)]