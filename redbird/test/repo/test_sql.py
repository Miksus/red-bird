
import datetime
import sys
import typing
import pytest
from redbird.repos import SQLRepo
from pydantic import BaseModel

try:
    from typing import Literal
except ImportError:
    from typing_extensions import Literal

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

def execute_sql(text, engine):
    import sqlalchemy
    with engine.begin() as conn:
        return conn.execute(sqlalchemy.text(text))

def test_init_engine():
    pytest.importorskip("sqlalchemy")
    from sqlalchemy import create_engine
    engine = create_engine('sqlite://')
    execute_sql(
        """CREATE TABLE pytest (
            id TEXT PRIMARY KEY,
            name TEXT,
            age INTEGER
        )""",
        engine=engine
    )
    repo = SQLRepo(engine=engine, table="pytest")
    assert repo.model_orm.__table__.name == "pytest"
    assert all(hasattr(repo.model_orm, col) for col in ("id", "name", "age"))

    repo.add({"id": "a", "name": "Jack", "age": 500})
    assert list(repo) == [dict(id="a", name="Jack", age=500)]

def test_init_conn_string(tmpdir):
    pytest.importorskip("sqlalchemy")
    from sqlalchemy import create_engine
    import sqlalchemy
    with tmpdir.as_cwd() as old_dir:
        conn_string = 'sqlite:///pytest.db'
        engine = create_engine(conn_string)
        execute_sql(
            """CREATE TABLE pytest (
                id TEXT PRIMARY KEY,
                name TEXT,
                age INTEGER
            )""",
            engine=engine
        )
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
    execute_sql("""CREATE TABLE pytest (
        id TEXT PRIMARY KEY,
        name TEXT,
        age INTEGER
    )""", engine=engine)
    repo = SQLRepo(session=session, table="pytest")
    assert repo.model_orm.__table__.name == "pytest"
    assert all(hasattr(repo.model_orm, col) for col in ("id", "name", "age"))

    repo.add({"id": "a", "name": "Jack", "age": 500})
    assert list(repo) == [dict(id="a", name="Jack", age=500)]

def test_init_model():
    pytest.importorskip("sqlalchemy")
    from sqlalchemy import create_engine
    engine = create_engine('sqlite://')
    execute_sql("""CREATE TABLE pytest (
        id TEXT PRIMARY KEY,
        name TEXT,
        age INTEGER
    )""", engine=engine)
    repo = SQLRepo(model=MyItem, engine=engine, table="pytest")
    assert repo.model is MyItem

    repo.add({"id": "a", "name": "Jack", "age": 500})
    assert list(repo) == [MyItem(id="a", name="Jack", age=500)]

def test_init_model_orm_mode():
    pytest.importorskip("sqlalchemy")
    from sqlalchemy import create_engine
    engine = create_engine('sqlite://')
    execute_sql("""CREATE TABLE pytest (
        id TEXT PRIMARY KEY,
        name TEXT,
        age INTEGER
    )""", engine=engine)
    repo = SQLRepo(model=MyItemWithORM, engine=engine, table="pytest")
    assert repo.model is MyItemWithORM

    repo.add({"id": "a", "name": "Jack", "age": 500})
    assert list(repo) == [MyItemWithORM(id="a", name="Jack", age=500)]

def test_init_orm():
    pytest.importorskip("sqlalchemy")
    from sqlalchemy import create_engine
    engine = create_engine('sqlite://')
    execute_sql("""CREATE TABLE pytest (
        id TEXT PRIMARY KEY,
        name TEXT,
        age INTEGER
    )""", engine=engine)
    repo = SQLRepo(model_orm=SQLItem, reflect_model=True, engine=engine)
    assert repo.model_orm is SQLItem
    assert issubclass(repo.model, BaseModel)

    repo.add({"id": "a", "name": "Jack", "age": 500})
    assert list(repo) == [repo.model(id="a", name="Jack", age=500)]

def test_init_model_and_orm():
    pytest.importorskip("sqlalchemy")
    from sqlalchemy import create_engine
    engine = create_engine('sqlite://')
    execute_sql("""CREATE TABLE pytest (
        id TEXT PRIMARY KEY,
        name TEXT,
        age INTEGER
    )""", engine=engine)
    repo = SQLRepo(model=MyItem, model_orm=SQLItem, engine=engine)
    assert repo.model_orm is SQLItem
    assert issubclass(repo.model, BaseModel)

    repo.add({"id": "a", "name": "Jack", "age": 500})
    assert list(repo) == [MyItem(id="a", name="Jack", age=500)]

def test_init_dict():
    pytest.importorskip("sqlalchemy")
    from sqlalchemy import create_engine
    engine = create_engine('sqlite://')
    execute_sql("""CREATE TABLE pytest (
        id TEXT PRIMARY KEY,
        name TEXT,
        age INTEGER
    )""", engine=engine)

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
        execute_sql("""CREATE TABLE pytest (
            id TEXT PRIMARY KEY,
            name TEXT,
            age INTEGER
        )""", engine=engine)

        with pytest.warns(DeprecationWarning):
            repo = SQLRepo.from_engine(model=dict, engine=engine, table="pytest")
        repo.add({"id": "a", "name": "Jack", "age": 500})
        assert list(repo) == [{"id": "a", "name": "Jack", "age": 500}]
        
        with pytest.warns(DeprecationWarning):
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
    
    execute_sql("""CREATE TABLE pytest (
        id TEXT,
        name TEXT,
        age INTEGER
    )""", engine=engine)
    with pytest.raises(KeyError):
        repo = SQLRepo(model=MyItem, engine=engine, table="mytable")

def test_init_reflect_model_id_field_in_model():
    pytest.importorskip("sqlalchemy")
    from sqlalchemy import create_engine
    engine = create_engine('sqlite://')
    class MyItem(BaseModel):
        __id_field__ = "name"
        name: str
        age: int

    repo = SQLRepo(model=MyItem, engine=engine, table="mytable", if_missing="create")
    assert repo.id_field == "name"

    # Make sure the __id_field__ is not in there
    repo.add(MyItem(name="Jack", age=20))
    obs = execute_sql("select * from mytable", engine=engine)
    assert list(obs) == [('Jack', 20)]

@pytest.mark.parametrize("cls,example_value", [
    pytest.param(str, "Jack", id="str"),
    pytest.param(int, 5, id="int"),
    pytest.param(float, 5.2, id="float"),

    pytest.param(datetime.date, datetime.date(2022, 1, 1), id="datetime.date"),
    pytest.param(datetime.datetime, datetime.datetime(2022, 1, 1, 12, 30), id="datetime.datetime"),
    pytest.param(datetime.timedelta, datetime.timedelta(days=2), id="datetime.timedelta", marks=pytest.mark.skip("SQLite does not support Interval")),

    pytest.param(dict, {"name": "Jack"}, id="dict"),

    pytest.param(typing.Optional[str], "Jack", id="Optional[str]"),
    pytest.param(typing.Union[str, None], "Jack", id="Union[str, None]", marks=pytest.mark.skipif(sys.version_info < (3, 8), reason="<py38")),
    pytest.param(Literal['yes', 'no'], "yes", id="Literal['...', '...']", marks=pytest.mark.skipif(sys.version_info < (3, 8), reason="<py38")),
    pytest.param(Literal[1, 2], 2, id="Literal[1, 2]", marks=pytest.mark.skipif(sys.version_info < (3, 8), reason="<py38")),
])
def test_init_type_insert(cls, example_value):
    pytest.importorskip("sqlalchemy")

    class MyItem(BaseModel):
        __id_field__ = "id"
        id: str
        myfield: cls

    from sqlalchemy import create_engine
    engine = create_engine('sqlite://')
    repo = SQLRepo(model=MyItem, engine=engine, table="mytable", if_missing="create", id_field="id")
    
    repo.add(MyItem(
        id="a",
        myfield=example_value
    ))
    obs = repo.filter_by().all()
    assert obs == [
        MyItem(
            id="a",
            myfield=example_value
        )
    ]

@pytest.mark.parametrize("cls,nullable,sql_type", [
    pytest.param(str, False, 'String', id="str"),
    pytest.param(int, False, 'Integer', id="int"),
    pytest.param(float, False, 'Float', id="float"),

    pytest.param(datetime.date, False, 'Date', id="datetime.date"),
    pytest.param(datetime.datetime, False, 'DateTime', id="datetime.datetime"),
    pytest.param(datetime.timedelta, False, 'Interval', id="datetime.timedelta", marks=pytest.mark.skip("SQLite does not support Interval")),

    pytest.param(dict, False, 'JSON', id="dict"),

    pytest.param(typing.Optional[str], True, 'String', id="Optional[str]"),
    pytest.param(typing.Union[str, None], True, 'String', id="Union[str, None]"),
    pytest.param(typing.Union[None, str], True, 'String', id="Union[None, str]"),
    pytest.param(Literal['yes', 'no'], False, 'String', id="Literal['...', '...']", marks=pytest.mark.skipif(sys.version_info < (3, 8), reason="<py38")),
    pytest.param(Literal[1, 2], False, 'Integer', id="Literal[1, 2]", marks=pytest.mark.skipif(sys.version_info < (3, 8), reason="<py38")),
    pytest.param(typing.Optional[Literal['yes', 'no']], True, 'String', id="Optional[Literal['...', '...']]", marks=pytest.mark.skipif(sys.version_info < (3, 8), reason="<py38")),
])
def test_init_column(cls, nullable, sql_type):
    pytest.importorskip("sqlalchemy")

    class MyItem(BaseModel):
        __id_field__ = "id"
        id: str
        myfield: cls
    import sqlalchemy
    from sqlalchemy import create_engine
    engine = create_engine('sqlite://')
    repo = SQLRepo(model=MyItem, engine=engine, table="mytable", if_missing="create", id_field="id")
    
    # Test column
    column = repo.model_orm.__table__.columns['myfield']
    assert column.nullable is nullable
    assert isinstance(column.type, getattr(sqlalchemy, sql_type))

@pytest.mark.parametrize("cls", [
    pytest.param(Literal[2, 'two'], id="Literal[2, 'two']", marks=pytest.mark.skipif(sys.version_info < (3, 8), reason="<py38")),
    pytest.param(typing.Union[str, int], id="Union[str, int]", marks=pytest.mark.skipif(sys.version_info < (3, 8), reason="<py38")),
    pytest.param(typing.Union[str, int, None], id="Union[str, int, None]", marks=pytest.mark.skipif(sys.version_info < (3, 8), reason="<py38")),
])
def test_init_column_fail(cls):
    pytest.importorskip("sqlalchemy")

    class MyItem(BaseModel):
        __id_field__ = "id"
        id: str
        myfield: cls

    from sqlalchemy import create_engine
    engine = create_engine('sqlite://')

    with pytest.raises(TypeError):
        repo = SQLRepo(model=MyItem, engine=engine, table="mytable", if_missing="create", id_field="id")

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