
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

def test_init_engine():
    pytest.importorskip("sqlalchemy")
    from sqlalchemy import create_engine
    engine = create_engine('sqlite://')
    engine.execute("""CREATE TABLE pytest (
        id TEXT PRIMARY KEY,
        name TEXT,
        age INTEGER
    )""")
    repo = SQLRepo(engine=engine, table="pytest")
    assert repo.model_orm.__table__.name == "pytest"
    assert all(hasattr(repo.model_orm, col) for col in ("id", "name", "age"))

    repo.add({"id": "a", "name": "Jack", "age": 500})
    assert list(repo) == [dict(id="a", name="Jack", age=500)]

def test_init_conn_string(tmpdir):
    pytest.importorskip("sqlalchemy")
    from sqlalchemy import create_engine
    with tmpdir.as_cwd() as old_dir:
        conn_string = 'sqlite:///pytest.db'
        engine = create_engine(conn_string)
        engine.execute("""CREATE TABLE pytest (
            id TEXT PRIMARY KEY,
            name TEXT,
            age INTEGER
        )""")
        repo = SQLRepo(conn_string=conn_string, table="pytest")
        assert repo.model_orm.__table__.name == "pytest"
        assert all(hasattr(repo.model_orm, col) for col in ("id", "name", "age"))

        repo.add({"id": "a", "name": "Jack", "age": 500})
        assert list(repo) == [dict(id="a", name="Jack", age=500)]

def test_init_session():
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

def test_init_model():
    pytest.importorskip("sqlalchemy")
    from sqlalchemy import create_engine
    engine = create_engine('sqlite://')
    engine.execute("""CREATE TABLE pytest (
        id TEXT PRIMARY KEY,
        name TEXT,
        age INTEGER
    )""")
    repo = SQLRepo(model=MyItem, engine=engine, table="pytest")
    assert repo.model is MyItem

    repo.add({"id": "a", "name": "Jack", "age": 500})
    assert list(repo) == [MyItem(id="a", name="Jack", age=500)]

def test_init_model_orm_mode():
    pytest.importorskip("sqlalchemy")
    from sqlalchemy import create_engine
    engine = create_engine('sqlite://')
    engine.execute("""CREATE TABLE pytest (
        id TEXT PRIMARY KEY,
        name TEXT,
        age INTEGER
    )""")
    repo = SQLRepo(model=MyItemWithORM, engine=engine, table="pytest")
    assert repo.model is MyItemWithORM

    repo.add({"id": "a", "name": "Jack", "age": 500})
    assert list(repo) == [MyItemWithORM(id="a", name="Jack", age=500)]

def test_init_orm():
    pytest.importorskip("sqlalchemy")
    from sqlalchemy import create_engine
    engine = create_engine('sqlite://')
    engine.execute("""CREATE TABLE pytest (
        id TEXT PRIMARY KEY,
        name TEXT,
        age INTEGER
    )""")
    repo = SQLRepo(model_orm=SQLItem, reflect_model=True, engine=engine)
    assert repo.model_orm is SQLItem
    assert issubclass(repo.model, BaseModel)

    repo.add({"id": "a", "name": "Jack", "age": 500})
    assert list(repo) == [repo.model(id="a", name="Jack", age=500)]

def test_init_model_and_orm():
    pytest.importorskip("sqlalchemy")
    from sqlalchemy import create_engine
    engine = create_engine('sqlite://')
    engine.execute("""CREATE TABLE pytest (
        id TEXT PRIMARY KEY,
        name TEXT,
        age INTEGER
    )""")
    repo = SQLRepo(model=MyItem, model_orm=SQLItem, engine=engine)
    assert repo.model_orm is SQLItem
    assert issubclass(repo.model, BaseModel)

    repo.add({"id": "a", "name": "Jack", "age": 500})
    assert list(repo) == [MyItem(id="a", name="Jack", age=500)]

def test_init_dict():
    pytest.importorskip("sqlalchemy")
    from sqlalchemy import create_engine
    engine = create_engine('sqlite://')
    engine.execute("""CREATE TABLE pytest (
        id TEXT PRIMARY KEY,
        name TEXT,
        age INTEGER
    )""")

    repo = SQLRepo(model=dict, engine=engine, table="pytest")
    repo.add({"id": "a", "name": "Jack", "age": 500})
    assert list(repo) == [{"id": "a", "name": "Jack", "age": 500}]

def test_init_fail_missing_connection():
    pytest.importorskip("sqlalchemy")
    with pytest.raises(TypeError):
        SQLRepo(model=MyItem, table="my_table")

def test_init_deprecated(tmpdir):
    with tmpdir.as_cwd() as old_dir:
        pytest.importorskip("sqlalchemy")
        from sqlalchemy import create_engine
        engine = create_engine('sqlite:///pytest.db')
        engine.execute("""CREATE TABLE pytest (
            id TEXT PRIMARY KEY,
            name TEXT,
            age INTEGER
        )""")

        repo = SQLRepo.from_engine(model=dict, engine=engine, table="pytest")
        repo.add({"id": "a", "name": "Jack", "age": 500})
        assert list(repo) == [{"id": "a", "name": "Jack", "age": 500}]
        
        repo = SQLRepo.from_connection_string(model=dict, conn_string="sqlite:///pytest.db", table="pytest")
        repo.add({"id": "b", "name": "Jack", "age": 5000})
        assert list(repo) == [{"id": "a", "name": "Jack", "age": 500}, {"id": "b", "name": "Jack", "age": 5000}]


def test_init_reflect_model_without_primary_key():
    # https://stackoverflow.com/a/23771348/13696660
    pytest.importorskip("sqlalchemy")
    from sqlalchemy import create_engine
    engine = create_engine('sqlite://')
    with pytest.raises(KeyError):
        repo = SQLRepo(model=MyItem, engine=engine, table="mytable", if_missing="create")
    
    engine.execute("""CREATE TABLE pytest (
        id TEXT,
        name TEXT,
        age INTEGER
    )""")
    with pytest.raises(KeyError):
        repo = SQLRepo(model=MyItem, engine=engine, table="mytable")

def test_init_create_table():
    pytest.importorskip("sqlalchemy")
    from sqlalchemy import create_engine
    from sqlalchemy.exc import NoSuchTableError
    engine = create_engine('sqlite://')
    with pytest.raises(NoSuchTableError):
        repo = SQLRepo(model=MyItem, engine=engine, table="mytable", id_field="id")
    with pytest.raises(NoSuchTableError):
        repo = SQLRepo(model=MyItem, engine=engine, table="mytable", id_field="id", if_missing="raise")

    repo = SQLRepo(model=MyItem, engine=engine, table="mytable", if_missing="create", id_field="id")
    assert issubclass(repo.model, BaseModel)

    repo.add({"id": "a", "name": "Jack", "age": 500})
    assert list(repo) == [MyItem(id="a", name="Jack", age=500)]