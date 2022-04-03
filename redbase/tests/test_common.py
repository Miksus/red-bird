
import configparser
from typing import Optional
import re
import json

import pytest
import responses
import requests
from redbase.repos.rest import RESTRepo
from redbase.repos.sqlalchemy import SQLRepo
from redbase.repos.memory import MemoryRepo
from redbase.repos.mongo import MongoRepo
from redbase.oper import greater_equal, greater_than, less_equal, less_than, not_equal

from sqlalchemy import Column, String, Integer, create_engine
from sqlalchemy.orm import declarative_base

from pydantic import BaseModel, Field

# ------------------------
# TEST ITEMS
# ------------------------

class PydanticItem(BaseModel):
    __colname__ = 'items'
    id: str
    name: str
    age: Optional[int]

class PydanticItemORM(BaseModel):
    id: str
    name: str
    age: Optional[int]
    class Config:
        orm_mode = True

class MongoItem(BaseModel):
    __colname__ = 'items'
    _id: str = Field(alias="id")
    id: str
    name: str
    age: int

SQLBase = declarative_base()

class SQLItem(SQLBase):
    __tablename__ = 'items'
    id = Column(String, primary_key=True)
    name = Column(String)
    age = Column(Integer)

    def __eq__(self, other):
        if not isinstance(other, SQLItem):
            return False
        return other.id == self.id and other.name == self.name and other.age == self.age

# ------------------------
# MOCK
# ------------------------

def get_mongo_uri():
    config = configparser.ConfigParser()
    config.read("redbase/tests/private.ini")
    pytest.importorskip("pymongo")
    return config["connection"]["mongodb"]

class RESTMock:

    def __init__(self):
        self.repo = MemoryRepo(PydanticItem)
    
    def post(self, request):
        data = json.loads(request.body)
        self.repo.add(data)
        return (200, {}, b"")

    def patch(self, request):
        data = json.loads(request.body)
        params = self.get_params(request)
        self.repo.filter_by(**params).update(**data)
        return (200, {}, b"")

    def patch_one(self, request):
        id = self.get_id(request)
        data = json.loads(request.body)
        assert "id" not in data

        data["id"] = id
        item = self.repo.model(**data)
        self.repo.update(item)
        return (200, {}, b"")

    def put(self, request):
        data = json.loads(request.body)
        item = self.repo.model(**data)
        self.repo.replace(item)
        return (200, {}, b"")

    def delete(self, request):
        params = self.get_params(request)
        self.repo.filter_by(**params).delete()
        return (200, {}, b"")

    def delete_one(self, request):
        id = self.get_id(request)
        del self.repo[id]
        return (200, {}, b"")

    def get(self, request):
        params = self.get_params(request)
        data = self.repo.filter_by(**params).all()
        data = [item.dict() for item in data]
        return (200, {"Content-Type": "application/json"}, json.dumps(data))

    def get_one(self, request):
        id = self.get_id(request)
        data = self.repo[id].dict()
        return (200, {"Content-Type": "application/json"}, json.dumps(data))

    def get_params(self, req):
        return {
            key: int(val) if val.isdigit() else val 
            for key, val in req.params.items()
        }

    def get_id(self, req):
        parts = req.url.rsplit("api/items/", 1)
        return parts[-1] if len(parts) > 1 else None

    def add_routes(self, rsps):
        rsps.add_callback(
            responses.POST, 
            'http://localhost:5000/api/items',
            callback=self.post,
            content_type='application/json',
        )
        rsps.add_callback(
            responses.PATCH, 
            re.compile('http://localhost:5000/api/items/[a-zA-Z]+'),
            callback=self.patch_one,
        )
        rsps.add_callback(
            responses.PATCH, 
            re.compile('http://localhost:5000/api/items?[a-zA-Z=_]+'),
            callback=self.patch,
        )

        rsps.add_callback(
            responses.PUT, 
            re.compile('http://localhost:5000/api/items'),
            callback=self.put,
        )

        rsps.add_callback(
            responses.DELETE, 
            'http://localhost:5000/api/items',
            callback=self.delete,
        )
        rsps.add_callback(
            responses.DELETE, 
            re.compile('http://localhost:5000/api/items/[a-zA-Z]+'),
            callback=self.delete_one,
        )

        rsps.add_callback(
            responses.GET, 
            re.compile('http://localhost:5000/api/items/[a-zA-Z]+'),
            callback=self.get_one,
        )
        rsps.add_callback(
            responses.GET, 
            re.compile('http://localhost:5000/api/items'),
            callback=self.get,
        )


def get_repo(type_):
    if type_ == "memory":
        repo = MemoryRepo(PydanticItem)

    elif type_ == "memory-dict":
        repo = MemoryRepo(dict)

    elif type_ == "sql":
        engine = create_engine('sqlite://')
        engine.execute("""CREATE TABLE pytest (
            id TEXT PRIMARY KEY,
            name TEXT,
            age INTEGER
        )""")
        repo = SQLRepo(engine=engine, table="pytest")

    elif type_ == "sql-orm":
        engine = create_engine('sqlite://')
        repo = SQLRepo(model_orm=SQLItem, engine=engine)
        repo.create()

    elif type_ == "sql-pydantic":
        engine = create_engine('sqlite://')
        repo = SQLRepo(PydanticItemORM, model_orm=SQLItem, engine=engine)
        SQLItem.__table__.create(bind=repo.session.bind)

    elif type_ == "mongo":
        repo = MongoRepo(PydanticItem, url=get_mongo_uri(), id_field="id")

        # Empty the collection
        pytest.importorskip("pymongo")
        from pymongo import MongoClient

        client = MongoClient(repo.session.url)
        col_name = repo.model.__colname__
        db = client.get_default_database()
        col = db[col_name]
        col.delete_many({})

    elif type_ == "http-rest":
        repo = RESTRepo(PydanticItem, url="http://localhost:5000/api/items", id_field="id")

    return repo

# ------------------------
# FIXTURES
# ------------------------

@pytest.fixture
def populated_repo(request):
    attrs = [
        dict(id="a", name="Jack", age=20),
        dict(id="b", name="John", age=30),
        dict(id="c", name="James", age=30),
        dict(id="d", name="Johnny", age=30),
        dict(id="e", name="Jesse", age=40),
    ]
    repo = get_repo(request.param)
    if request.param == "memory":
        repo.collection = [repo.model(**item_attrs) for item_attrs in attrs]
        yield repo
    elif request.param == "memory-dict":
        repo.collection = [item_attrs for item_attrs in attrs]
        yield repo
    elif request.param == "sql":
        for item_attrs in attrs:
            item = repo.model_orm(**item_attrs)
            repo.session.add(item)
        repo.session.commit()
        yield repo
    elif request.param == "sql-orm":
        for item_attrs in attrs:
            item = SQLItem(**item_attrs)
            repo.session.add(item)
        repo.session.commit()
        yield repo
    elif request.param == "sql-pydantic":
        for item_attrs in attrs:
            item = SQLItem(**item_attrs)
            repo.session.add(item)
        repo.session.commit()
        yield repo
    elif request.param == "mongo":
        pytest.importorskip("pymongo")
        from pymongo import MongoClient

        client = MongoClient(repo.session.url)
        col_name = repo.model.__colname__
        db = client.get_default_database()
        col = db[col_name]
        col.delete_many({})
        for item in attrs:
            item["_id"] = item.pop("id")
        col.insert_many(attrs)
        yield repo
    elif request.param == "http-rest":
        api = RESTMock()
        with responses.RequestsMock(assert_all_requests_are_fired=False) as rsps:
            api.add_routes(rsps)
            api.repo.collection = [repo.model(**item_attrs) for item_attrs in attrs]
            yield repo

@pytest.fixture
def repo(request):
    repo = get_repo(request.param)
    if request.param == "http-rest":
        api = RESTMock()
        with responses.RequestsMock(assert_all_requests_are_fired=False) as rsps:
            api.add_routes(rsps)
            yield repo
    else:
        yield repo

TEST_CASES = [
    pytest.param("memory"),
    pytest.param("memory-dict"),
    pytest.param("sql"),
    pytest.param("sql-orm"),
    pytest.param("sql-pydantic"),
    pytest.param("mongo"),
    pytest.param("http-rest"),
]


# ------------------------
# ACTUAL TESTS
# ------------------------


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
    'populated_repo',
    TEST_CASES,
    indirect=True
)
class TestPopulated:

    def test_filter_by_first(self, populated_repo):
        repo = populated_repo
        Item = repo.model
        assert repo.filter_by(age=30).first() == Item(id="b", name="John", age=30)

    def test_filter_by_last(self, populated_repo):
        repo = populated_repo
        Item = repo.model
        assert repo.filter_by(age=30).last() == Item(id="d", name="Johnny", age=30)


    def test_filter_by_limit(self, populated_repo):
        repo = populated_repo
        Item = repo.model
        assert repo.filter_by(age=30).limit(2) == [
            Item(id="b", name="John", age=30),
            Item(id="c", name="James", age=30),
        ]

    def test_filter_by_all(self, populated_repo):
        repo = populated_repo
        Item = repo.model
        assert repo.filter_by(age=30).all() == [
            Item(id="b", name="John", age=30),
            Item(id="c", name="James", age=30),
            Item(id="d", name="Johnny", age=30),
        ]

    def test_filter_by_update(self, populated_repo):
        repo = populated_repo
        Item = repo.model

        repo.filter_by(age=30).update(name="Something")
        assert repo.filter_by().all() == [
            Item(id="a", name="Jack", age=20),
            Item(id="b", name="Something", age=30),
            Item(id="c", name="Something", age=30),
            Item(id="d", name="Something", age=30),
            Item(id="e", name="Jesse", age=40),
        ]

    def test_filter_by_delete(self, populated_repo):
        repo = populated_repo
        Item = repo.model

        repo.filter_by(age=30).delete()
        assert repo.filter_by().all() == [
            Item(id="a", name="Jack", age=20),
            #Item(id="b", name="John", age=30),
            #Item(id="c", name="James", age=30),
            #Item(id="d", name="Johnny", age=30),
            Item(id="e", name="Jesse", age=40),
        ]

    def test_filter_by_count(self, populated_repo):
        repo = populated_repo
        Item = repo.model
        assert repo.filter_by(age=30).count() == 3

    def test_filter_by_less_than(self, populated_repo):
        repo = populated_repo
        Item = repo.model
        assert repo.filter_by(age=less_than(40)).all() == [
            Item(id="a", name="Jack", age=20),
            Item(id="b", name="John", age=30),
            Item(id="c", name="James", age=30),
            Item(id="d", name="Johnny", age=30),
            #Item(id="e", name="Jesse", age=40),
        ]

    def test_getitem(self, populated_repo):
        repo = populated_repo
        Item = repo.model

        assert repo["b"] == Item(id="b", name="John", age=30)

    def test_getitem_missing(self, populated_repo):
        repo = populated_repo
        with pytest.raises(KeyError):
            repo["not_found"]

    def test_delitem(self, populated_repo):
        repo = populated_repo
        Item = repo.model

        del repo["b"]
        assert repo.filter_by().all() == [
            Item(id="a", name="Jack", age=20),
            #Item(id="b", name="John", age=30),
            Item(id="c", name="James", age=30),
            Item(id="d", name="Johnny", age=30),
            Item(id="e", name="Jesse", age=40),
        ]

    def test_delitem_missing(self, populated_repo):
        repo = populated_repo
        with pytest.raises(KeyError):
            del repo["not_found"]

    def test_setitem(self, populated_repo):
        repo = populated_repo
        Item = repo.model

        repo["d"] = {"name": "Johnny boy"}

        assert repo.filter_by().all() == [
            Item(id="a", name="Jack", age=20),
            Item(id="b", name="John", age=30),
            Item(id="c", name="James", age=30),
            Item(id="d", name="Johnny boy", age=30),
            Item(id="e", name="Jesse", age=40),
        ]

    def test_setitem_missing(self, populated_repo):
        repo = populated_repo
        with pytest.raises(KeyError):
            repo["not_found"] = {"name": "something"}


@pytest.mark.parametrize(
    'populated_repo',
    TEST_CASES,
    indirect=True
)
class TestFilteringOperations:

    def test_greater_than(self, populated_repo):
        repo = populated_repo
        Item = repo.model
        assert repo.filter_by(age=greater_than(20)).all() == [
            #Item(id="a", name="Jack", age=20),
            Item(id="b", name="John", age=30),
            Item(id="c", name="James", age=30),
            Item(id="d", name="Johnny", age=30),
            Item(id="e", name="Jesse", age=40),
        ]

    def test_less_than(self, populated_repo):
        repo = populated_repo
        Item = repo.model
        assert repo.filter_by(age=less_than(30)).all() == [
            Item(id="a", name="Jack", age=20),
            #Item(id="b", name="John", age=30),
            #Item(id="c", name="James", age=30),
            #Item(id="d", name="Johnny", age=30),
            #Item(id="e", name="Jesse", age=40),
        ]

    def test_greater_equal(self, populated_repo):
        repo = populated_repo
        Item = repo.model
        assert repo.filter_by(age=greater_equal(30)).all() == [
            #Item(id="a", name="Jack", age=20),
            Item(id="b", name="John", age=30),
            Item(id="c", name="James", age=30),
            Item(id="d", name="Johnny", age=30),
            Item(id="e", name="Jesse", age=40),
        ]

    def test_less_equal(self, populated_repo):
        repo = populated_repo
        Item = repo.model
        assert repo.filter_by(age=less_equal(30)).all() == [
            Item(id="a", name="Jack", age=20),
            Item(id="b", name="John", age=30),
            Item(id="c", name="James", age=30),
            Item(id="d", name="Johnny", age=30),
            #Item(id="e", name="Jesse", age=40),
        ]

    def test_not_equal(self, populated_repo):
        repo = populated_repo
        Item = repo.model
        assert repo.filter_by(age=not_equal(30)).all() == [
            Item(id="a", name="Jack", age=20),
            #Item(id="b", name="John", age=30),
            #Item(id="c", name="James", age=30),
            #Item(id="d", name="Johnny", age=30),
            Item(id="e", name="Jesse", age=40),
        ]

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
        assert repo.filter_by().all() == [
            Item(id="a", name="Jack", age=20),
            Item(id="b", name="John", age=30)
        ]

    def test_update(self, repo):
        Item = repo.model

        repo.add(Item(id="a", name="Jack", age=20))
        repo.add(Item(id="b", name="John", age=30))

        repo.update(Item(id="a", name="Max"))
        items = repo.filter_by().all()
        assert items == [
            Item(id="a", name="Max", age=20),
            Item(id="b", name="John", age=30),
        ]

    def test_replace(self, repo):
        Item = repo.model

        repo.add(Item(id="a", name="Jack", age=20))
        repo.add(Item(id="b", name="John", age=30))

        repo.replace(Item(id="a", name="Max"))

        items = sorted(repo.filter_by().all(), key=lambda x: repo.get_field_value(x, "id"))
        assert items == [
            Item(id="a", name="Max"),
            Item(id="b", name="John", age=30),
        ]

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