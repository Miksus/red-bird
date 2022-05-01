
import pytest
from redbird.repos import SQLRepo
from pydantic import BaseModel

class MyItem(BaseModel):
    id: str
    name: str
    age: int

class MyItemWithORM(BaseModel):
    id: str
    name: str
    age: int
    class Config:
        orm_mode = True

try:
    from sqlalchemy.orm import declarative_base
    from sqlalchemy import Column, String, Integer
except ImportError:
    pass
else:
    SQLBase = declarative_base()

    class SQLItem(SQLBase):
        __tablename__ = 'pytest'
        id = Column(String, primary_key=True)
        name = Column(String)
        age = Column(Integer)

        def __eq__(self, other):
            if not isinstance(other, SQLItem):
                return False
            return other.id == self.id and other.name == self.name and other.age == self.age

def test_minimal():
    pytest.importorskip("sqlalchemy")
    from sqlalchemy import create_engine
    engine = create_engine('sqlite://')
    engine.execute("""CREATE TABLE pytest (
        id TEXT PRIMARY KEY,
        name TEXT,
        age INTEGER
    )""")
    repo = SQLRepo.from_engine(engine=engine, table="pytest")
    assert repo.model_orm.__table__.name == "pytest"
    assert all(hasattr(repo.model_orm, col) for col in ("id", "name", "age"))

    repo.add({"id": "a", "name": "Jack", "age": 500})
    assert list(repo) == [dict(id="a", name="Jack", age=500)]

def test_from_session():
    pytest.importorskip("sqlalchemy")
    from sqlalchemy import create_engine
    from sqlalchemy.orm import Session
    engine = create_engine('sqlite://')
    session = Session(engine)
    engine.execute("""CREATE TABLE pytest (
        id TEXT PRIMARY KEY,
        name TEXT,
        age INTEGER
    )""")
    repo = SQLRepo(session=session, table="pytest")
    assert repo.model_orm.__table__.name == "pytest"
    assert all(hasattr(repo.model_orm, col) for col in ("id", "name", "age"))

    repo.add({"id": "a", "name": "Jack", "age": 500})
    assert list(repo) == [dict(id="a", name="Jack", age=500)]

def test_with_model():
    pytest.importorskip("sqlalchemy")
    from sqlalchemy import create_engine
    engine = create_engine('sqlite://')
    engine.execute("""CREATE TABLE pytest (
        id TEXT PRIMARY KEY,
        name TEXT,
        age INTEGER
    )""")
    repo = SQLRepo.from_engine(model=MyItem, engine=engine, table="pytest")
    assert repo.model is MyItem

    repo.add({"id": "a", "name": "Jack", "age": 500})
    assert list(repo) == [MyItem(id="a", name="Jack", age=500)]

def test_with_model_orm_mode():
    pytest.importorskip("sqlalchemy")
    from sqlalchemy import create_engine
    engine = create_engine('sqlite://')
    engine.execute("""CREATE TABLE pytest (
        id TEXT PRIMARY KEY,
        name TEXT,
        age INTEGER
    )""")
    repo = SQLRepo.from_engine(model=MyItemWithORM, engine=engine, table="pytest")
    assert repo.model is MyItemWithORM

    repo.add({"id": "a", "name": "Jack", "age": 500})
    assert list(repo) == [MyItemWithORM(id="a", name="Jack", age=500)]

def test_with_orm():
    pytest.importorskip("sqlalchemy")
    from sqlalchemy import create_engine
    engine = create_engine('sqlite://')
    engine.execute("""CREATE TABLE pytest (
        id TEXT PRIMARY KEY,
        name TEXT,
        age INTEGER
    )""")
    repo = SQLRepo.from_engine(model_orm=SQLItem, reflect_model=True, engine=engine)
    assert repo.model_orm is SQLItem
    assert issubclass(repo.model, BaseModel)

    repo.add({"id": "a", "name": "Jack", "age": 500})
    assert list(repo) == [repo.model(id="a", name="Jack", age=500)]

def test_with_model_and_orm():
    pytest.importorskip("sqlalchemy")
    from sqlalchemy import create_engine
    engine = create_engine('sqlite://')
    engine.execute("""CREATE TABLE pytest (
        id TEXT PRIMARY KEY,
        name TEXT,
        age INTEGER
    )""")
    repo = SQLRepo.from_engine(model=MyItem, model_orm=SQLItem, engine=engine)
    assert repo.model_orm is SQLItem
    assert issubclass(repo.model, BaseModel)

    repo.add({"id": "a", "name": "Jack", "age": 500})
    assert list(repo) == [MyItem(id="a", name="Jack", age=500)]

def test_with_dict():
    pytest.importorskip("sqlalchemy")
    from sqlalchemy import create_engine
    engine = create_engine('sqlite://')
    engine.execute("""CREATE TABLE pytest (
        id TEXT PRIMARY KEY,
        name TEXT,
        age INTEGER
    )""")

    repo = SQLRepo.from_engine(model=dict, engine=engine, table="pytest")
    repo.add({"id": "a", "name": "Jack", "age": 500})
    assert list(repo) == [{"id": "a", "name": "Jack", "age": 500}]