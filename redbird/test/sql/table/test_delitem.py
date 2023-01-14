import pytest

@pytest.mark.parametrize("qry,results", [
    pytest.param(
        "b", [
            {"index_1": "a", "index_2": 1, "column_1": "Jack", "column_2": 10},
            {"index_1": "a", "index_2": 2, "column_1": "John", "column_2": 9},
            #{"index_1": "b", "index_2": 1, "column_1": "James", "column_2": 8},
            #{"index_1": "b", "index_2": 2, "column_1": "Johnny", "column_2": 7},
            {"index_1": "c", "index_2": 1, "column_1": "Jimmy", "column_2": 8},
            {"index_1": "c", "index_2": 2, "column_1": "Jim", "column_2": 7},
            {"index_1": "d", "index_2": 1, "column_1": "Bon", "column_2": 99},
            {"index_1": "d", "index_2": 2, "column_1": "Bam", "column_2": 98},
        ],
        id="range item (string)"
    ),
    pytest.param(
        ["b", "d"], [
            {"index_1": "a", "index_2": 1, "column_1": "Jack", "column_2": 10},
            {"index_1": "a", "index_2": 2, "column_1": "John", "column_2": 9},
            #{"index_1": "b", "index_2": 1, "column_1": "James", "column_2": 8},
            #{"index_1": "b", "index_2": 2, "column_1": "Johnny", "column_2": 7},
            {"index_1": "c", "index_2": 1, "column_1": "Jimmy", "column_2": 8},
            {"index_1": "c", "index_2": 2, "column_1": "Jim", "column_2": 7},
            #{"index_1": "d", "index_2": 1, "column_1": "Bon", "column_2": 99},
            #{"index_1": "d", "index_2": 2, "column_1": "Bam", "column_2": 98},
        ],
        id="range item (list of strings)"
    ),
    pytest.param(
        ("b",), 
        [
            {"index_1": "a", "index_2": 1, "column_1": "Jack", "column_2": 10},
            {"index_1": "a", "index_2": 2, "column_1": "John", "column_2": 9},
            #{"index_1": "b", "index_2": 1, "column_1": "James", "column_2": 8},
            #{"index_1": "b", "index_2": 2, "column_1": "Johnny", "column_2": 7},
            {"index_1": "c", "index_2": 1, "column_1": "Jimmy", "column_2": 8},
            {"index_1": "c", "index_2": 2, "column_1": "Jim", "column_2": 7},
            {"index_1": "d", "index_2": 1, "column_1": "Bon", "column_2": 99},
            {"index_1": "d", "index_2": 2, "column_1": "Bam", "column_2": 98},
        ],
        id="range item (tuple)"
    ),
    pytest.param(
        [("b",), ("d",)], 
        [
            {"index_1": "a", "index_2": 1, "column_1": "Jack", "column_2": 10},
            {"index_1": "a", "index_2": 2, "column_1": "John", "column_2": 9},
            #{"index_1": "b", "index_2": 1, "column_1": "James", "column_2": 8},
            #{"index_1": "b", "index_2": 2, "column_1": "Johnny", "column_2": 7},
            {"index_1": "c", "index_2": 1, "column_1": "Jimmy", "column_2": 8},
            {"index_1": "c", "index_2": 2, "column_1": "Jim", "column_2": 7},
            #{"index_1": "d", "index_2": 1, "column_1": "Bon", "column_2": 99},
            #{"index_1": "d", "index_2": 2, "column_1": "Bam", "column_2": 98},
        ],
        id="range item (list of tuple)"
    ),
    pytest.param(
        slice("b", "c"), 
        [
            {"index_1": "a", "index_2": 1, "column_1": "Jack", "column_2": 10},
            {"index_1": "a", "index_2": 2, "column_1": "John", "column_2": 9},
            #{"index_1": "b", "index_2": 1, "column_1": "James", "column_2": 8},
            #{"index_1": "b", "index_2": 2, "column_1": "Johnny", "column_2": 7},
            #{"index_1": "c", "index_2": 1, "column_1": "Jimmy", "column_2": 8},
            #{"index_1": "c", "index_2": 2, "column_1": "Jim", "column_2": 7},
            {"index_1": "d", "index_2": 1, "column_1": "Bon", "column_2": 99},
            {"index_1": "d", "index_2": 2, "column_1": "Bam", "column_2": 98},
        ],
        id="range item (slice)"
    ),
    pytest.param(
        slice("b", None), 
        [
            {"index_1": "a", "index_2": 1, "column_1": "Jack", "column_2": 10},
            {"index_1": "a", "index_2": 2, "column_1": "John", "column_2": 9},
        ],
        id="range item (slice, left closed)"
    ),
    pytest.param(
        slice(None, "c"), 
        [
            {"index_1": "d", "index_2": 1, "column_1": "Bon", "column_2": 99},
            {"index_1": "d", "index_2": 2, "column_1": "Bam", "column_2": 98},
        ],
        id="range item (slice, right closed)"
    ),
    pytest.param(
        slice(None, None, None), 
        [],
        id="range item (open)"
    ),
    pytest.param(
        ("b", 1), 
        [
            {"index_1": "a", "index_2": 1, "column_1": "Jack", "column_2": 10},
            {"index_1": "a", "index_2": 2, "column_1": "John", "column_2": 9},
            #{"index_1": "b", "index_2": 1, "column_1": "James", "column_2": 8},
            {"index_1": "b", "index_2": 2, "column_1": "Johnny", "column_2": 7},
            {"index_1": "c", "index_2": 1, "column_1": "Jimmy", "column_2": 8},
            {"index_1": "c", "index_2": 2, "column_1": "Jim", "column_2": 7},
            {"index_1": "d", "index_2": 1, "column_1": "Bon", "column_2": 99},
            {"index_1": "d", "index_2": 2, "column_1": "Bam", "column_2": 98},
        ],
        id="single item (tuple)"
    ),
    pytest.param(
        [("b", 1), ("c", 2)], 
        [
            {"index_1": "a", "index_2": 1, "column_1": "Jack", "column_2": 10},
            {"index_1": "a", "index_2": 2, "column_1": "John", "column_2": 9},
            #{"index_1": "b", "index_2": 1, "column_1": "James", "column_2": 8},
            {"index_1": "b", "index_2": 2, "column_1": "Johnny", "column_2": 7},
            {"index_1": "c", "index_2": 1, "column_1": "Jimmy", "column_2": 8},
            #{"index_1": "c", "index_2": 2, "column_1": "Jim", "column_2": 7},
            {"index_1": "d", "index_2": 1, "column_1": "Bon", "column_2": 99},
            {"index_1": "d", "index_2": 2, "column_1": "Bam", "column_2": 98},
        ],
        id="single items (list of tuples)"
    ),
])
def test_delitem_multiindex(table_multiindex, qry, results):
    table = table_multiindex
    del table[qry]
    assert list(table.select()) == results

@pytest.mark.parametrize("qry,results", [
    pytest.param(
        "a", 
        [
            #{"index_1": "a", "column_1": "Jack", "column_2": 10},
            {"index_1": "b", "column_1": "James", "column_2": 8},
            {"index_1": "c", "column_1": "Jimmy", "column_2": 8},
        ],
        id="single item"
    ),
    pytest.param(
        ["a", "b"], 
        [
            #{"index_1": "a", "column_1": "Jack", "column_2": 10},
            #{"index_1": "b", "column_1": "James", "column_2": 8},
            {"index_1": "c", "column_1": "Jimmy", "column_2": 8},
        ],
        id="multi item"
    ),
    pytest.param(
        slice(None, None, None), 
        [],
        id="slice (open)"
    ),
    pytest.param(
        slice("b", None, None),
        [
            {"index_1": "a", "column_1": "Jack", "column_2": 10},
            #{"index_1": "b", "column_1": "James", "column_2": 8},
            #{"index_1": "c", "column_1": "Jimmy", "column_2": 8},
        ],
        id="slice (left closed)"
    ),
    pytest.param(
        slice(None, "b", None),
        [
            #{"index_1": "a", "column_1": "Jack", "column_2": 10},
            #{"index_1": "b", "column_1": "James", "column_2": 8},
            {"index_1": "c", "column_1": "Jimmy", "column_2": 8},
        ],
        id="slice (right closed)"
    ),
])
def test_delitem_singleindex(table_singleindex, qry, results):
    table = table_singleindex
    del table[qry]
    assert list(table.select()) == results

def test_delitem_error(table_multiindex, table_singleindex):

    # Delete single index
    with pytest.raises(KeyError):
        del table_singleindex["not_found"]

    with pytest.raises(KeyError):
        del table_multiindex["not_found"]

    with pytest.raises(KeyError):
        del table_multiindex[("a", -999)]

    with pytest.raises(KeyError):
        del table_multiindex[("zzz",)]

    with pytest.raises(IndexError):
        del table_singleindex[("a", 2)]

    with pytest.raises(IndexError):
        del table_multiindex[("a", 2, "not_found")]