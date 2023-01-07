from typing import Optional
from pydantic import BaseModel
from datetime import date
import pytest

from redbird.sql import Table, create_table

def test_create(engine):
    create_table(table="mytable", columns=["col1", "col2"], engine=engine)

def test_create_model(engine):
    class MyModel(BaseModel):
        name: str
        score: int = 999
        birth_date: date
        color: Optional[str]

    tbl = Table("mytable", engine=engine)
    tbl.create_from_model(MyModel)
    res = engine.execute("PRAGMA table_info(mytable);")
    assert [
        {'cid': 0, 'name': 'name', 'type': 'VARCHAR', 'notnull': 1, 'dflt_value': None, 'pk': 0},
        {'cid': 1, 'name': 'score', 'type': 'INTEGER', 'notnull': 0, 'dflt_value': None, 'pk': 0},
        {'cid': 2, 'name': 'birth_date', 'type': 'DATE', 'notnull': 1, 'dflt_value': None, 'pk': 0},
        {'cid': 3, 'name': 'color', 'type': 'VARCHAR', 'notnull': 0, 'dflt_value': None, 'pk': 0},
    ] == list(res.mappings())

    tbl.insert({"name": "Jack", "birth_date": date(2000, 1, 1)})
    assert list(tbl.select()) == [{'name': 'Jack', 'score': 999, 'birth_date': date(2000, 1, 1), 'color': None}]

def test_create_model_primary_key(engine):
    class MyModel(BaseModel):
        myid: str

    tbl = Table("mytable", engine=engine)
    tbl.create_from_model(MyModel, primary_column="myid")
    res = engine.execute("PRAGMA table_info(mytable);")
    assert [
        {'cid': 0, 'name': 'myid', 'type': 'VARCHAR', 'notnull': 1, 'dflt_value': None, 'pk': 1},
    ] == list(res.mappings())

def test_create_model_multi_primary_key(engine):
    class MyModel(BaseModel):
        myid1: str
        myid2: int

    tbl = Table("mytable", engine=engine)
    tbl.create_from_model(MyModel, primary_column=("myid1", "myid2"))
    res = engine.execute("PRAGMA table_info(mytable);")
    assert [
        {'cid': 0, 'name': 'myid1', 'type': 'VARCHAR', 'notnull': 1, 'dflt_value': None, 'pk': 1},
        {'cid': 1, 'name': 'myid2', 'type': 'INTEGER', 'notnull': 1, 'dflt_value': None, 'pk': 2},
    ] == list(res.mappings())

def test_create_dict(engine):

    tbl = Table("mytable", engine=engine)
    tbl.create({
        "mycol_1": str, 
        "mycol_2": int, 
        "mycol_3": date,
    })
    res = engine.execute("PRAGMA table_info(mytable);")
    assert [
        {'cid': 0, 'name': 'mycol_1', 'type': 'VARCHAR', 'notnull': 0, 'dflt_value': None, 'pk': 0},
        {'cid': 1, 'name': 'mycol_2', 'type': 'INTEGER', 'notnull': 0, 'dflt_value': None, 'pk': 0},
        {'cid': 2, 'name': 'mycol_3', 'type': 'DATE', 'notnull': 0, 'dflt_value': None, 'pk': 0},
    ] == list(res.mappings())

def test_create_list(engine):
    sqlalchemy = pytest.importorskip("sqlalchemy")
    tbl = Table("mytable", engine=engine)
    tbl.create([
        sqlalchemy.Column("mycol_1", sqlalchemy.VARCHAR, primary_key=True),
        sqlalchemy.Column("mycol_2", sqlalchemy.INTEGER, nullable=False),
        sqlalchemy.Column("mycol_3", sqlalchemy.DATE),
    ])
    res = engine.execute("PRAGMA table_info(mytable);")
    assert [
        {'cid': 0, 'name': 'mycol_1', 'type': 'VARCHAR', 'notnull': 1, 'dflt_value': None, 'pk': 1},
        {'cid': 1, 'name': 'mycol_2', 'type': 'INTEGER', 'notnull': 1, 'dflt_value': None, 'pk': 0},
        {'cid': 2, 'name': 'mycol_3', 'type': 'DATE', 'notnull': 0, 'dflt_value': None, 'pk': 0},
    ] == list(res.mappings())
