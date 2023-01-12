import pytest
from redbird.sql import Table


@pytest.fixture()
def table_singleindex(engine) -> Table:
    tbl = Table(name="getitems_singleindex", bind=engine)
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
def table_multiindex(engine) -> Table:
    tbl = Table(name="getitems", bind=engine)
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
        {"index_1": "d", "index_2": 1, "column_1": "Bon", "column_2": 99},
        {"index_1": "d", "index_2": 2, "column_1": "Bam", "column_2": 98},
    ])
    return tbl