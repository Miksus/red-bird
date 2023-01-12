import pytest
from redbird.sql import Table

def test_multi(table_multiindex, table_singleindex):
    # Single index
    assert table_singleindex[["a", "b"]] == [
        {"index_1": "a", "column_1": "Jack", "column_2": 10},
        {"index_1": "b", "column_1": "James", "column_2": 8},
    ]

    # Multi indices
    assert table_multiindex["a"] == [
        {"index_1": "a", "index_2": 1, "column_1": "Jack", "column_2": 10},
        {"index_1": "a", "index_2": 2, "column_1": "John", "column_2": 9},
    ]

    assert table_multiindex[[("a", 1), ("b", 2)]] == [
        {"index_1": "a", "index_2": 1, "column_1": "Jack", "column_2": 10},
        {"index_1": "b", "index_2": 2, "column_1": "Johnny", "column_2": 7},
    ]
    assert table_multiindex[["a", "b"]] == [
        {"index_1": "a", "index_2": 1, "column_1": "Jack", "column_2": 10},
        {"index_1": "a", "index_2": 2, "column_1": "John", "column_2": 9},
        {"index_1": "b", "index_2": 1, "column_1": "James", "column_2": 8},
        {"index_1": "b", "index_2": 2, "column_1": "Johnny", "column_2": 7},
    ]

def test_single_item(table_multiindex, table_singleindex):
    # Single index
    assert table_singleindex["a"] == {"index_1": "a", "column_1": "Jack", "column_2": 10}

    # Multiple indices
    assert table_multiindex[("a", 1)] == {"index_1": "a", "index_2": 1, "column_1": "Jack", "column_2": 10}
    assert table_multiindex["a", 1] == {"index_1": "a", "index_2": 1, "column_1": "Jack", "column_2": 10}

def test_slice(table_multiindex, table_singleindex):
    # Single index
    assert table_singleindex["a":"b"] == [
        {"index_1": "a", "column_1": "Jack", "column_2": 10},
        {"index_1": "b", "column_1": "James", "column_2": 8},
    ]
    assert table_singleindex[:"b"] == [
        {"index_1": "a", "column_1": "Jack", "column_2": 10},
        {"index_1": "b", "column_1": "James", "column_2": 8},
    ]


    # Multi-indices
    assert table_multiindex[:] == [
        {"index_1": "a", "index_2": 1, "column_1": "Jack", "column_2": 10},
        {"index_1": "a", "index_2": 2, "column_1": "John", "column_2": 9},
        {"index_1": "b", "index_2": 1, "column_1": "James", "column_2": 8},
        {"index_1": "b", "index_2": 2, "column_1": "Johnny", "column_2": 7},
        {"index_1": "c", "index_2": 1, "column_1": "Jimmy", "column_2": 8},
        {"index_1": "c", "index_2": 2, "column_1": "Jim", "column_2": 7},
        {"index_1": "d", "index_2": 1, "column_1": "Bon", "column_2": 99},
        {"index_1": "d", "index_2": 2, "column_1": "Bam", "column_2": 98},
    ]

    assert table_multiindex["a":"c"] == [
        {"index_1": "a", "index_2": 1, "column_1": "Jack", "column_2": 10},
        {"index_1": "a", "index_2": 2, "column_1": "John", "column_2": 9},
        {"index_1": "b", "index_2": 1, "column_1": "James", "column_2": 8},
        {"index_1": "b", "index_2": 2, "column_1": "Johnny", "column_2": 7},
        {"index_1": "c", "index_2": 1, "column_1": "Jimmy", "column_2": 8},
        {"index_1": "c", "index_2": 2, "column_1": "Jim", "column_2": 7},
    ]

    assert table_multiindex["b":] == [
        {"index_1": "b", "index_2": 1, "column_1": "James", "column_2": 8},
        {"index_1": "b", "index_2": 2, "column_1": "Johnny", "column_2": 7},
        {"index_1": "c", "index_2": 1, "column_1": "Jimmy", "column_2": 8},
        {"index_1": "c", "index_2": 2, "column_1": "Jim", "column_2": 7},
        {"index_1": "d", "index_2": 1, "column_1": "Bon", "column_2": 99},
        {"index_1": "d", "index_2": 2, "column_1": "Bam", "column_2": 98},
    ]
    assert table_multiindex[:"b"] == [
        {"index_1": "a", "index_2": 1, "column_1": "Jack", "column_2": 10},
        {"index_1": "a", "index_2": 2, "column_1": "John", "column_2": 9},
        {"index_1": "b", "index_2": 1, "column_1": "James", "column_2": 8},
        {"index_1": "b", "index_2": 2, "column_1": "Johnny", "column_2": 7},
    ]
    assert table_multiindex[:"0"] == []
    assert table_multiindex["x":] == []
    assert table_multiindex["x":"z"] == []

def test_getitem_error(table_multiindex):

    with pytest.raises(KeyError):
        # Not found, tuple (second key)
        table_multiindex[("a", "not_found")]

    with pytest.raises(KeyError):
        # Not found, range
        table_multiindex["not_found"]

    with pytest.raises(KeyError):
        # Not found, tuple (first key)
        table_multiindex[("not_found", 2)]

    with pytest.raises(KeyError):
        # Not found, list of tuples
        table_multiindex[[("not_found", 2)]]

    with pytest.raises(IndexError):
        # Too long for index
        table_multiindex[("a", 2, "long")]

    with pytest.raises(IndexError):
        # Varying sizes
        table_multiindex[[("a", 2), ("a",)]]

    with pytest.raises(IndexError):
        # Varying sizes
        table_multiindex[[("a", 2), "a"]]

    with pytest.raises(TypeError):
        # Tuple in tuple
        table_multiindex[("a", 2), "a"]

    with pytest.raises(ValueError):
        # Tuple in tuple
        table_multiindex[::2]

def test_getitem_no_primary_key(engine):
    tbl = Table(name="getitems_singleindex", bind=engine)
    tbl.create([
        {"name": "index_1", "type_": str, "primary_key": False},
        {"name": "column_1", "type_": str, "primary_key": False},
        {"name": "column_2", "type_": int, "primary_key": False},
    ])
    tbl.insert([
        {"index_1": "a", "column_1": "Jack", "column_2": 10},
        {"index_1": "b", "column_1": "James", "column_2": 8},
        {"index_1": "c", "column_1": "Jimmy", "column_2": 8},
    ])
    with pytest.raises(TypeError):
        tbl[("a", 1)]

