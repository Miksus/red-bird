
import configparser
from typing import Optional
import re, os
import json
from dotenv import load_dotenv

import pytest
import responses
import requests
import mongomock
from redbird.repos.csv import CSVFileRepo
from redbird.repos.json import JSONDirectoryRepo
from redbird.repos.rest import RESTRepo
from redbird.repos.sqlalchemy import SQLExprRepo, SQLRepo
from redbird.repos.memory import MemoryRepo
from redbird.repos.mongo import MongoRepo
from redbird.oper import greater_equal, greater_than, less_equal, less_than, not_equal

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
    id: str  = Field(alias="_id")
    name: str
    age: int

class PydanticItemWithDefaults(BaseModel):
    __id_field__ = 'id'
    id: str
    name: str
    age: Optional[int] = 20
    color: str = Field(default_factory=lambda: "black")

try:
    from sqlalchemy import Column, String, Integer, create_engine
    from sqlalchemy.orm import declarative_base
except ImportError:
    # Has no SQLAlchemy
    pass
else:
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
    load_dotenv()
    pytest.importorskip("pymongo")
    if "MONGO_CONN" not in os.environ:
        pytest.skip()
    return os.environ["MONGO_CONN"]

class RESTMock:

    def __init__(self):
        self.repo = MemoryRepo(model=PydanticItem, id_field="id")
    
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
        self.repo.get_by(id).update(**data)
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


def get_repo(type_, tmpdir, model=PydanticItem):
    if type_.startswith("sql-"):
        pytest.importorskip("sqlalchemy")
        from sqlalchemy import Column, String, Integer, create_engine
        import sqlalchemy
    if type_ == "memory":
        repo = MemoryRepo(model=model, id_field="id")

    elif type_ == "memory-dict":
        repo = MemoryRepo(model=dict, id_field="id")

    elif type_ == "csv":
        repo = CSVFileRepo(model=model, id_field="id", filename=str(tmpdir / "repo.csv"))
        repo.create()

    elif type_ == "json-dir":
        repo = JSONDirectoryRepo(model=model, id_field="id", path=str(tmpdir))

    elif type_ == "sql-dict":
        engine = create_engine('sqlite://')
        with engine.begin() as conn:
            conn.execute(sqlalchemy.text("""
            CREATE TABLE pytest (
                id TEXT PRIMARY KEY,
                name TEXT,
                age INTEGER
            )"""))
        repo = SQLRepo(engine=engine, table="pytest", id_field="id")

    elif type_ == "sql-pydantic":
        engine = create_engine('sqlite://')
        repo = SQLRepo(model=model, engine=engine, table="pytest", id_field="id", if_missing='create')
        #SQLItem.__table__.create(bind=repo.session.bind)

    elif type_ == "sql-orm":
        engine = create_engine('sqlite://')
        repo = SQLRepo(model_orm=SQLItem, engine=engine, table="items", id_field="id")
        repo.create()

    elif type_ == "sql-expr":
        engine = create_engine(f'sqlite:///{tmpdir}/logs.db')
        repo = SQLExprRepo(model=model, engine=engine, table="items", id_field="id")
        repo.create()

    elif type_ == "sql-pydantic-orm":
        engine = create_engine('sqlite://')
        repo = SQLRepo(model=PydanticItemORM, model_orm=SQLItem, engine=engine, id_field="id")
        SQLItem.__table__.create(bind=repo.session.bind)

    elif type_ == "mongo-mock":
        pytest.importorskip("pymongo")
        repo = MongoRepo(model=model, uri="mongodb://localhost:27017/pytest?authSource=admin", database="pytest", collection="items", id_field="id")
 
    elif type_ == "mongo":
        pytest.importorskip("pymongo")
        repo = MongoRepo(model=model, uri=get_mongo_uri(), database="pytest", collection="items", id_field="id")

        # Empty the collection
        import pymongo

        client = pymongo.MongoClient(repo.session.url)
        col_name = repo.model.__colname__
        db = client.get_default_database()
        col = db[col_name]
        col.delete_many({})

    elif type_ == "http-rest":
        repo = RESTRepo(model=model, url="http://localhost:5000/api/items", id_field="id")

    return repo

# ------------------------
# FIXTURES
# ------------------------

@pytest.fixture
def repo(request, tmpdir):
    repo = get_repo(request.param, tmpdir)
    if request.param == "http-rest":
        api = RESTMock()
        with responses.RequestsMock(assert_all_requests_are_fired=False) as rsps:
            api.add_routes(rsps)
            yield repo
    elif request.param == "mongo-mock":
        with mongomock.patch(servers=(('localhost', 27017),)):
            yield repo
    else:
        yield repo

@pytest.fixture
def repo_defaults(request, tmpdir):
    repo = get_repo(request.param, tmpdir, model=PydanticItemWithDefaults)
    if request.param == "http-rest":
        api = RESTMock()
        with responses.RequestsMock(assert_all_requests_are_fired=False) as rsps:
            api.add_routes(rsps)
            yield repo
    elif request.param == "mongo-mock":
        with mongomock.patch(servers=(('localhost', 27017),)):
            yield repo
    else:
        yield repo