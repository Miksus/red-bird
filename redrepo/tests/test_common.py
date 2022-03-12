
import configparser

import pytest
from redrepo.ext.sqlalchemy import SQLAlchemyRepo
from redrepo.ext.memory import ListRepo
from redrepo.ext.mongo import MongoRepo

from sqlalchemy import Column, String, Integer, create_engine
from sqlalchemy.orm import declarative_base

from pydantic import BaseModel

class PydanticItem(BaseModel):
    __colname__ = 'items'
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

def get_mongo_uri():
    config = configparser.ConfigParser()
    config.read("redrepo/tests/private.ini")
    pytest.importorskip("pymongo")
    return config["connection"]["mongodb"]

def get_repo(type_):
    if type_ == "memory":
        repo = ListRepo(PydanticItem)
        return repo
    elif type_ == "sql":
        engine = create_engine('sqlite://')
        repo = SQLAlchemyRepo(SQLItem, engine=engine)
        repo.create()
        return repo
    elif type_ == "mongo":
        repo = MongoRepo(PydanticItem, url=get_mongo_uri(), id_field="id")

        # Empty the collection
        pytest.importorskip("pymongo")
        from pymongo import MongoClient

        client = MongoClient(repo.session.url)
        col_name = repo.cls_item.__colname__
        db = client.get_default_database()
        col = db[col_name]
        col.delete_many({})

        return repo

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
        repo.store = [repo.cls_item(**item_attrs) for item_attrs in attrs]
    elif request.param == "sql":
        for item_attrs in attrs:
            item = repo.cls_item(**item_attrs)
            repo.session.add(item)
        repo.session.commit()
    elif request.param == "mongo":
        pytest.importorskip("pymongo")
        from pymongo import MongoClient

        client = MongoClient(repo.session.url)
        col_name = repo.cls_item.__colname__
        db = client.get_default_database()
        col = db[col_name]
        col.delete_many({})
        col.insert_many(attrs)
    return repo

@pytest.fixture
def repo(request):
    return get_repo(request.param)


@pytest.mark.parametrize(
    'populated_repo',
    [
        pytest.param("memory"),
        pytest.param("sql"),
        pytest.param("mongo"),
    ],
    indirect=True
)
class TestPopulated:

    def test_filter_by(self, populated_repo):
        repo = populated_repo
        Item = repo.cls_item

        assert repo.filter_by(age=30).first() == Item(id="b", name="John", age=30)
        assert repo.filter_by(age=30).last() == Item(id="d", name="Johnny", age=30)
        assert repo.filter_by(age=30).all() == [
            Item(id="b", name="John", age=30),
            Item(id="c", name="James", age=30),
            Item(id="d", name="Johnny", age=30),
        ]

        assert repo.filter_by(age=30).limit(2) == [
            Item(id="b", name="John", age=30),
            Item(id="c", name="James", age=30),
        ]

    def test_getitem(self, populated_repo):
        repo = populated_repo
        Item = repo.cls_item

        assert repo["b"] == Item(id="b", name="John", age=30)
        with pytest.raises(KeyError):
            repo["not_found"]

    def test_delitem(self, populated_repo):
        repo = populated_repo
        Item = repo.cls_item

        del repo["b"]
        assert repo.filter_by().all() == [
            Item(id="a", name="Jack", age=20),
            #Item(id="b", name="John", age=30),
            Item(id="c", name="James", age=30),
            Item(id="d", name="Johnny", age=30),
            Item(id="e", name="Jesse", age=40),
        ]
        with pytest.raises(KeyError):
            del repo["not_found"]


@pytest.mark.parametrize(
    'repo',
    [
        pytest.param("memory"),
        pytest.param("sql"),
        pytest.param("mongo"),
    ],
    indirect=True
)
class TestEmpty:

    def test_add(self, repo):
        Item = repo.cls_item
        
        assert repo.filter_by().all() == []

        repo.add(Item(id="a", name="Jack", age=20))
        assert repo.filter_by().all() == [Item(id="a", name="Jack", age=20)]

        repo.add(Item(id="b", name="John", age=30))
        assert repo.filter_by().all() == [
            Item(id="a", name="Jack", age=20),
            Item(id="b", name="John", age=30)
        ]