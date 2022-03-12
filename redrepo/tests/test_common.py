
import configparser

import pytest
from redrepo.ext.sqlalchemy import SQLAlchemyRepo
from redrepo.ext.memory import ListRepo
from redrepo.ext.mongo import MongoRepo
from redrepo.operation import greater_than, less_than

from sqlalchemy import Column, String, Integer, create_engine
from sqlalchemy.orm import declarative_base

from pydantic import BaseModel, Field

class PydanticItem(BaseModel):
    __colname__ = 'items'
    id: str
    name: str
    age: int

class PydanticItemORM(BaseModel):
    __tablename__ = 'items'
    id: str
    name: str
    age: int
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
        repo = SQLAlchemyRepo(model_orm=SQLItem, engine=engine)
        repo.create()
        return repo
    elif type_ == "sql-pydantic":
        engine = create_engine('sqlite://')
        repo = SQLAlchemyRepo(PydanticItemORM, model_orm=SQLItem, engine=engine)
        SQLItem.__table__.create(bind=repo.session.bind)
        return repo
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
        repo.store = [repo.model(**item_attrs) for item_attrs in attrs]
    elif request.param == "sql":
        for item_attrs in attrs:
            item = SQLItem(**item_attrs)
            repo.session.add(item)
        repo.session.commit()
    elif request.param == "sql-pydantic":
        for item_attrs in attrs:
            item = SQLItem(**item_attrs)
            repo.session.add(item)
        repo.session.commit()
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
    return repo

@pytest.fixture
def repo(request):
    return get_repo(request.param)


@pytest.mark.parametrize(
    'populated_repo',
    [
        pytest.param("memory"),
        pytest.param("sql"),
        pytest.param("sql-pydantic"),
        pytest.param("mongo"),
    ],
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

    def test_filter_by_greater_than(self, populated_repo):
        repo = populated_repo
        Item = repo.model
        assert repo.filter_by(age=greater_than(20)).all() == [
            #Item(id="a", name="Jack", age=20),
            Item(id="b", name="John", age=30),
            Item(id="c", name="James", age=30),
            Item(id="d", name="Johnny", age=30),
            Item(id="e", name="Jesse", age=40),
        ]

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

        repo.update(Item(id="a", name="Max", age=50))
        assert repo.filter_by().all() == [
            Item(id="a", name="Max", age=50),
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