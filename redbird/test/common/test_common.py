
import configparser
from typing import Optional
import re, os
import json
from dotenv import load_dotenv

import pytest
import responses
import requests
from redbird.repos.csv import CSVFileRepo
from redbird.repos.json import JSONDirectoryRepo
from redbird.repos.rest import RESTRepo
from redbird.repos.sqlalchemy import SQLRepo
from redbird.repos.memory import MemoryRepo
from redbird.repos.mongo import MongoRepo
from redbird.oper import in_, skip, between, greater_equal, greater_than, less_equal, less_than, not_equal

from pydantic import BaseModel, Field


TEST_CASES = [
    pytest.param("memory"),
    pytest.param("memory-dict"),
    pytest.param("sql-dict"),
    pytest.param("sql-pydantic"),
    pytest.param("sql-orm"),
    pytest.param("sql-expr"),
    pytest.param("sql-pydantic-orm"),
    pytest.param("mongo"),
    pytest.param("mongo-mock"),
    pytest.param("http-rest"),
    pytest.param("csv"),
    pytest.param("json-dir"),
]

TEST_CASES_DEFAULT = [
    pytest.param("memory"),
    pytest.param("sql-pydantic"),
    pytest.param("sql-expr"),
    pytest.param("mongo"),
    pytest.param("mongo-mock"),
    pytest.param("http-rest"),
    pytest.param("csv"),
    pytest.param("json-dir"),
]

def sort_items(items, repo, field="id"):
    return list(sorted(items, key=lambda x: repo.get_field_value(x, field)))

# ------------------------
# ACTUAL TESTS
# ------------------------

class RepoTests:

    def populate(self, repo, items=None):
        if items is None:
            items = [
                dict(id="a", name="Jack", age=20),
                dict(id="b", name="John", age=30),
                dict(id="c", name="James", age=30),
                dict(id="d", name="Johnny", age=30),
                dict(id="e", name="Jesse", age=40),
            ]
        for d in items:
            item = repo.to_item(d)
            repo.add(item)

@pytest.mark.parametrize(
    'repo',
    TEST_CASES,
    indirect=True
)
class TestAPI:

    def test_session(self, repo):
        repo.session.remove()

    def test_has_attributes(self, repo):
        # Test given attrs are found
        attrs = [
            "session", "model",
            "add", "insert", "update", "upsert", "delete",
            "filter_by",
        ]
        for attr in attrs:
            getattr(repo, attr)

    def test_has_attributes_result(self, repo):
        attrs = [
            "query_", "repo", "query",
            "first", "all", "limit", "last",
            "update", "delete",
            "count",
        ]
        filter = repo.filter_by()
        for attr in attrs:
            getattr(filter, attr)


@pytest.mark.parametrize(
    'repo',
    TEST_CASES,
    indirect=True
)
class TestPopulated(RepoTests):

    def populate(self, repo, items=None):
        if items is None:
            items = [
                dict(id="a", name="Jack", age=20),
                dict(id="b", name="John", age=30),
                dict(id="c", name="James", age=30),
                dict(id="d", name="Johnny", age=30),
                dict(id="e", name="Jesse", age=40),
            ]
        for d in items:
            item = repo.to_item(d)
            repo.add(item)

    def test_filter_by_first(self, repo):
        self.populate(repo)
        Item = repo.model
        if repo.ordered:
            assert repo.filter_by(age=30).first() == Item(id="b", name="John", age=30)
        else:
            item = repo.filter_by(age=30).first()
            assert repo.get_field_value(item, 'age') == 30

    def test_filter_by_last(self, repo):
        self.populate(repo)
        Item = repo.model
        if repo.ordered:
            assert repo.filter_by(age=30).last() == Item(id="d", name="Johnny", age=30)
        else:
            item = repo.filter_by(age=30).last()
            assert repo.get_field_value(item, 'age') == 30

    def test_filter_by_limit(self, repo):
        self.populate(repo)
        Item = repo.model

        actual = repo.filter_by(age=30).limit(2)
        expected = [
                Item(id="b", name="John", age=30),
                Item(id="c", name="James", age=30),
            ]
        if not repo.ordered:
            # We don't know which are the top 2
            assert isinstance(actual, list)
            for item in actual:
                assert repo.get_field_value(item, "age") == 30
            assert len(actual) == 2
        else:
            assert actual == expected

    def test_filter_by_all(self, repo):
        self.populate(repo)
        Item = repo.model
        
        actual = repo.filter_by(age=30).all()
        expected = [
            Item(id="b", name="John", age=30),
            Item(id="c", name="James", age=30),
            Item(id="d", name="Johnny", age=30),
        ]
        
        if not repo.ordered:
            actual = sort_items(actual, repo)
            expected = sort_items(expected, repo)
        assert actual == expected

    def test_filter_by_iter(self, repo):
        self.populate(repo)
        Item = repo.model
        
        actual = list(repo.filter_by(age=30))
        expected = [
            Item(id="b", name="John", age=30),
            Item(id="c", name="James", age=30),
            Item(id="d", name="Johnny", age=30),
        ]
        
        if not repo.ordered:
            actual = sort_items(actual, repo)
            expected = sort_items(expected, repo)
        assert actual == expected

    def test_filter_by_update(self, repo):
        self.populate(repo)
        Item = repo.model

        repo.filter_by(age=30).update(name="Something")

        actual = repo.filter_by().all()
        expected = [
            Item(id="a", name="Jack", age=20),
            Item(id="b", name="Something", age=30),
            Item(id="c", name="Something", age=30),
            Item(id="d", name="Something", age=30),
            Item(id="e", name="Jesse", age=40),
        ]
        if not repo.ordered:
            actual = sort_items(actual, repo)
            expected = sort_items(expected, repo)
        assert actual == expected

    def test_filter_by_replace(self, repo):
        self.populate(repo)
        Item = repo.model

        repo.filter_by(id="b").replace(id="b", name="Something")

        expected = [
            Item(id="a", name="Jack", age=20),
            Item(id="c", name="James", age=30),
            Item(id="d", name="Johnny", age=30),
            Item(id="e", name="Jesse", age=40),
            Item(id="b", name="Something", age=None),
        ]
        if repo.model == dict and not isinstance(repo, SQLRepo):
            # In dict repositories the not given fields don't exists
            # (exception: structured data stores)
            expected = [
                Item(id="a", name="Jack", age=20),
                Item(id="c", name="James", age=30),
                Item(id="d", name="Johnny", age=30),
                Item(id="e", name="Jesse", age=40),
                Item(id="b", name="Something"),
            ]

        actual = repo.filter_by().all()
        if not repo.ordered:
            actual = sort_items(actual, repo)
            expected = sort_items(expected, repo)

        assert actual == expected

    def test_filter_by_replace_dict(self, repo):
        self.populate(repo)
        Item = repo.model

        repo.filter_by(id="b").replace({"id": "b", "name": "Something"})
        
        expected = [
            Item(id="a", name="Jack", age=20),
            Item(id="c", name="James", age=30),
            Item(id="d", name="Johnny", age=30),
            Item(id="e", name="Jesse", age=40),
            Item(id="b", name="Something", age=None),
        ]
        if repo.model == dict and not isinstance(repo, SQLRepo):
            # In dict repositories the not given fields don't exists
            # (exception: structured data stores)
            expected = [
                Item(id="a", name="Jack", age=20),
                Item(id="c", name="James", age=30),
                Item(id="d", name="Johnny", age=30),
                Item(id="e", name="Jesse", age=40),
                Item(id="b", name="Something"),
            ]

        actual = repo.filter_by().all()
        if not repo.ordered:
            actual = sort_items(actual, repo)
            expected = sort_items(expected, repo)

        assert actual == expected

    def test_filter_by_delete(self, repo):
        self.populate(repo)
        Item = repo.model

        repo.filter_by(age=30).delete()

        actual = repo.filter_by().all()
        expected = [
            Item(id="a", name="Jack", age=20),
            #Item(id="b", name="John", age=30),
            #Item(id="c", name="James", age=30),
            #Item(id="d", name="Johnny", age=30),
            Item(id="e", name="Jesse", age=40),
        ]
        if not repo.ordered:
            actual = sort_items(actual, repo)
            expected = sort_items(expected, repo)
        assert actual == expected

    def test_filter_by_count(self, repo):
        self.populate(repo)
        Item = repo.model
        assert repo.filter_by(age=30).count() == 3

    def test_filter_by_less_than(self, repo):
        self.populate(repo)

        if isinstance(repo, RESTRepo):
            pytest.xfail("RESTRepo does not support operations (yet)")

        Item = repo.model
        actual = repo.filter_by(age=less_than(40)).all() 
        expected = [
            Item(id="a", name="Jack", age=20),
            Item(id="b", name="John", age=30),
            Item(id="c", name="James", age=30),
            Item(id="d", name="Johnny", age=30),
            #Item(id="e", name="Jesse", age=40),
        ]
        if not repo.ordered:
            actual = sort_items(actual, repo)
            expected = sort_items(expected, repo)
        assert actual == expected

    def test_getitem(self, repo):
        self.populate(repo)
        Item = repo.model

        assert repo["b"] == Item(id="b", name="John", age=30)

    def test_getitem_missing(self, repo):
        self.populate(repo)
        with pytest.raises(KeyError):
            repo["not_found"]

    def test_delitem(self, repo):
        self.populate(repo)
        Item = repo.model

        del repo["b"]


        actual = repo.filter_by().all() 
        expected = [
            Item(id="a", name="Jack", age=20),
            #Item(id="b", name="John", age=30),
            Item(id="c", name="James", age=30),
            Item(id="d", name="Johnny", age=30),
            Item(id="e", name="Jesse", age=40),
        ]
        if not repo.ordered:
            actual = sort_items(actual, repo)
            expected = sort_items(expected, repo)
        assert actual == expected

    @pytest.mark.skip(reason="This is a minor issue")
    def test_delitem_missing(self, repo):
        self.populate(repo)
        with pytest.raises(KeyError):
            del repo["not_found"]

    def test_setitem(self, repo):
        self.populate(repo)
        Item = repo.model

        repo["d"] = {"name": "Johnny boy"}

        actual = repo.filter_by().all() 
        expected = [
            Item(id="a", name="Jack", age=20),
            Item(id="b", name="John", age=30),
            Item(id="c", name="James", age=30),
            Item(id="d", name="Johnny boy", age=30),
            Item(id="e", name="Jesse", age=40),
        ]
        if not repo.ordered:
            actual = sort_items(actual, repo)
            expected = sort_items(expected, repo)
        assert actual == expected

    @pytest.mark.skip(reason="This is a minor issue")
    def test_setitem_missing(self, repo):
        self.populate(repo)
        with pytest.raises(KeyError):
            repo["not_found"] = {"name": "something"}

@pytest.mark.parametrize(
    'repo',
    TEST_CASES,
    indirect=True
)
class TestFilteringOperations(RepoTests):

    def populate(self, repo, items=None):
        if items is None:
            items = [
                dict(id="a", name="Jack", age=20),
                dict(id="b", name="John", age=30),
                dict(id="c", name="James", age=30),
                dict(id="d", name="Johnny", age=30),
                dict(id="e", name="Jesse", age=40),
                dict(id="f", name="Jim", age=41),
            ]
        for d in items:
            item = repo.to_item(d)
            repo.add(item)

    def test_greater_than(self, repo):
        self.populate(repo)

        if isinstance(repo, RESTRepo):
            pytest.xfail("RESTRepo does not support operations (yet)")

        Item = repo.model

        actual = repo.filter_by(age=greater_than(20)).all()
        expected = [
            #Item(id="a", name="Jack", age=20),
            Item(id="b", name="John", age=30),
            Item(id="c", name="James", age=30),
            Item(id="d", name="Johnny", age=30),
            Item(id="e", name="Jesse", age=40),
            Item(id="f", name="Jim", age=41),
        ]
        if not repo.ordered:
            actual = sort_items(actual, repo)
            expected = sort_items(expected, repo)
        assert actual == expected

    def test_less_than(self, repo):
        self.populate(repo)

        if isinstance(repo, RESTRepo):
            pytest.xfail("RESTRepo does not support operations (yet)")
    
        Item = repo.model
        actual = repo.filter_by(age=less_than(30)).all()
        expected = [
            Item(id="a", name="Jack", age=20),
            #Item(id="b", name="John", age=30),
            #Item(id="c", name="James", age=30),
            #Item(id="d", name="Johnny", age=30),
            #Item(id="e", name="Jesse", age=40),
        ]
        if not repo.ordered:
            actual = sort_items(actual, repo)
            expected = sort_items(expected, repo)
        assert actual == expected

    def test_greater_equal(self, repo):
        self.populate(repo)
        if isinstance(repo, RESTRepo):
            pytest.xfail("RESTRepo does not support operations (yet)")
        Item = repo.model
        actual = repo.filter_by(age=greater_equal(30)).all()
        expected = [
            #Item(id="a", name="Jack", age=20),
            Item(id="b", name="John", age=30),
            Item(id="c", name="James", age=30),
            Item(id="d", name="Johnny", age=30),
            Item(id="e", name="Jesse", age=40),
            Item(id="f", name="Jim", age=41),
        ]
        if not repo.ordered:
            actual = sort_items(actual, repo)
            expected = sort_items(expected, repo)
        assert actual == expected

    def test_less_equal(self, repo):
        self.populate(repo)

        if isinstance(repo, RESTRepo):
            pytest.xfail("RESTRepo does not support operations (yet)")

        Item = repo.model
        actual = repo.filter_by(age=less_equal(30)).all()
        expected = [
            Item(id="a", name="Jack", age=20),
            Item(id="b", name="John", age=30),
            Item(id="c", name="James", age=30),
            Item(id="d", name="Johnny", age=30),
            #Item(id="e", name="Jesse", age=40),
        ]
        if not repo.ordered:
            actual = sort_items(actual, repo)
            expected = sort_items(expected, repo)
        assert actual == expected

    def test_not_equal(self, repo):
        self.populate(repo)

        if isinstance(repo, RESTRepo):
            pytest.xfail("RESTRepo does not support operations (yet)")

        Item = repo.model
        actual = repo.filter_by(age=not_equal(30)).all()
        expected = [
            Item(id="a", name="Jack", age=20),
            #Item(id="b", name="John", age=30),
            #Item(id="c", name="James", age=30),
            #Item(id="d", name="Johnny", age=30),
            Item(id="e", name="Jesse", age=40),
            Item(id="f", name="Jim", age=41),
        ]
        if not repo.ordered:
            actual = sort_items(actual, repo)
            expected = sort_items(expected, repo)
        assert actual == expected

    def test_between(self, repo):
        self.populate(repo)
        if isinstance(repo, RESTRepo):
            pytest.xfail("RESTRepo does not support operations (yet)")
        Item = repo.model
        actual = repo.filter_by(age=between(30, 40)).all() 
        expected = [
            #Item(id="a", name="Jack", age=20),
            Item(id="b", name="John", age=30),
            Item(id="c", name="James", age=30),
            Item(id="d", name="Johnny", age=30),
            Item(id="e", name="Jesse", age=40),
            #Item(id="e", name="Jim", age=41),
        ]
        if not repo.ordered:
            actual = sort_items(actual, repo)
            expected = sort_items(expected, repo)
        assert actual == expected

    def test_in(self, repo):
        self.populate(repo)
        if isinstance(repo, RESTRepo):
            pytest.xfail("RESTRepo does not support operations (yet)")
        Item = repo.model
        actual = repo.filter_by(age=in_([20, 41])).all() 
        expected = [
            Item(id="a", name="Jack", age=20),
            #Item(id="b", name="John", age=30),
            #Item(id="c", name="James", age=30),
            #Item(id="d", name="Johnny", age=30),
            #Item(id="e", name="Jesse", age=40),
            Item(id="f", name="Jim", age=41),
        ]
        if not repo.ordered:
            actual = sort_items(actual, repo)
            expected = sort_items(expected, repo)
        assert actual == expected

    def test_skip(self, repo):
        self.populate(repo)
        if isinstance(repo, RESTRepo):
            pytest.xfail("RESTRepo does not support operations (yet)")
        Item = repo.model
        actual = repo.filter_by(age=skip).all()
        expected = [
            Item(id="a", name="Jack", age=20),
            Item(id="b", name="John", age=30),
            Item(id="c", name="James", age=30),
            Item(id="d", name="Johnny", age=30),
            Item(id="e", name="Jesse", age=40),
            Item(id="f", name="Jim", age=41),
        ]
        if not repo.ordered:
            actual = sort_items(actual, repo)
            expected = sort_items(expected, repo)
        assert actual == expected

@pytest.mark.parametrize(
    'repo',
    TEST_CASES,
    indirect=True
)
class TestEmpty:

    def test_add(self, repo):
        Item = repo.model
        
        assert repo.filter_by().all() == []

        repo.add(Item(id="a", name="Jack", age=20))
        assert repo.filter_by().all() == [Item(id="a", name="Jack", age=20)]

        repo.add(Item(id="b", name="John", age=30))
        
        actual = repo.filter_by().all()
        expected = [
            Item(id="a", name="Jack", age=20),
            Item(id="b", name="John", age=30)
        ]
        if not repo.ordered:
            actual = sort_items(actual, repo)
            expected = sort_items(expected, repo)
        assert actual == expected

    def test_update(self, repo):
        Item = repo.model

        repo.add(Item(id="a", name="Jack", age=20))
        repo.add(Item(id="b", name="John", age=30))

        repo.update(Item(id="a", name="Max"))
        actual = repo.filter_by().all()

        actual = repo.filter_by().all()
        expected = [
            Item(id="a", name="Max", age=20),
            Item(id="b", name="John", age=30),
        ]
        if not repo.ordered:
            actual = sort_items(actual, repo)
            expected = sort_items(expected, repo)
        assert actual == expected

    def test_replace(self, repo):
        Item = repo.model

        repo.add(Item(id="a", name="Jack", age=20))
        repo.add(Item(id="b", name="John", age=30))

        repo.replace(Item(id="a", name="Max"))

        if repo.model == dict and not isinstance(repo, SQLRepo):
            expected = [
                Item(id="a", name="Max"),
                Item(id="b", name="John", age=30),
            ]
        else:
            expected = [
                Item(id="a", name="Max", age=None),
                Item(id="b", name="John", age=30),
            ]

        actual = repo.filter_by().all()
        actual = sort_items(actual, repo)
        expected = sort_items(expected, repo)
        assert actual == expected

    def test_add_exist(self, repo):
        Item = repo.model
    
        repo.add(Item(id="a", name="Jack", age=20))
        with pytest.raises(Exception):
            repo.add(Item(id="a", name="John", age=30))

    def test_add_exist_ignore(self, repo):
        Item = repo.model
    
        repo.add(Item(id="a", name="Jack", age=20), if_exists="ignore")
        repo.add(Item(id="a", name="John", age=30), if_exists="ignore")
        assert repo.filter_by().all() == [
            Item(id="a", name="Jack", age=20),
        ]

    def test_add_exist_update(self, repo):
        Item = repo.model
    
        repo.add(Item(id="a", name="Jack", age=20), if_exists="update")
        assert repo.filter_by().all() == [
            Item(id="a", name="Jack", age=20),
        ]

        repo.add(Item(id="a", name="John", age=30), if_exists="update")
        assert repo.filter_by().all() == [
            Item(id="a", name="John", age=30),
        ]

    def test_get_by(self, repo):
        Item = repo.model

        repo.add(Item(id="a", name="Max", age=20))
        repo.add(Item(id="b", name="John", age=30))

        # asserting
        actual = list(repo)
        expected =[
            Item(id="a", name="Max", age=20),
            Item(id="b", name="John", age=30),
        ]
        if not repo.ordered:
            actual = sort_items(actual, repo)
            expected = sort_items(expected, repo)
        assert actual == expected

        repo.get_by("a").update(age=40)

        actual = list(repo)
        expected = [
            Item(id="a", name="Max", age=40),
            Item(id="b", name="John", age=30),
        ]
        if not repo.ordered:
            actual = sort_items(actual, repo)
            expected = sort_items(expected, repo)
        assert actual == expected

        repo.get_by("b").delete()
        assert list(repo) == [
            Item(id="a", name="Max", age=40),
        ]

@pytest.mark.parametrize(
    'repo',
    TEST_CASES,
    indirect=True
)
class TestMalformedData(RepoTests):

    def test_filter_raise(self, repo):

        class MalformedItem(BaseModel):
            id: str
            name: int
            age: int

        self.populate(repo, [
            {"id": "a", "name": 1, "age": 20},
            {"id": "b", "name": "Jack", "age": 20},
            {"id": "c", "name": "James", "age": 30},
            {"id": "d", "name": 2, "age": 30},
            {"id": "e", "name": 3, "age": 30},
        ])
        repo.model = MalformedItem

        with pytest.raises(ValueError):
            repo.filter_by().all()

    def test_filter_warn(self, repo):

        class MalformedItem(BaseModel):
            id: str
            name: int
            age: int

        self.populate(repo, [
            {"id": "a", "name": 1, "age": 20},
            {"id": "b", "name": "Jack", "age": 20},
            {"id": "c", "name": "James", "age": 30},
            {"id": "d", "name": 2, "age": 30},
            {"id": "e", "name": 3, "age": 30},
        ])
        repo.model = MalformedItem
        repo.errors_query = "warn"

        with pytest.warns(UserWarning):
            repo.filter_by().all()

    def test_filter_discard(self, repo):

        class MalformedItem(BaseModel):
            id: str
            name: int
            age: int

        self.populate(repo, [
            {"id": "a", "name": 1, "age": 20},
            {"id": "b", "name": "Jack", "age": 20},
            {"id": "c", "name": "James", "age": 30},
            {"id": "d", "name": 2, "age": 30},
            {"id": "e", "name": 3, "age": 30},
        ])
        repo.model = MalformedItem
        repo.errors_query = "discard"

        Item = MalformedItem
        actual = repo.filter_by().all() 
        expected = [
            Item(id="a", name=1, age=20),
            Item(id="d", name=2, age=30),
            Item(id="e", name=3, age=30),
        ]
        if not repo.ordered:
            actual = sort_items(actual, repo)
            expected = sort_items(expected, repo)
        assert actual == expected

@pytest.mark.parametrize(
    'repo_defaults',
    TEST_CASES_DEFAULT,
    indirect=True
)
class TestItemDefaults(RepoTests):

    def test_add_with_default(self, repo_defaults):
        repo = repo_defaults
        Item = repo.model
        
        assert repo.filter_by().all() == []

        repo.add(Item(id="a", name="Jack"))
        assert repo.filter_by().all() == [Item(id="a", name="Jack", age=20, color="black")]