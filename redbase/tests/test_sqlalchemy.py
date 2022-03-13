import pytest
from redbase.ext.sqlalchemy import SQLRepo
from pydantic import BaseModel
from sqlalchemy.orm import declarative_base
from sqlalchemy import Column, Integer, String, create_engine
from sqlalchemy.orm import sessionmaker

Base = declarative_base()

class MyItem(Base):
    __tablename__ = 'items'
    id = Column(String, primary_key=True)
    name = Column(String)
    age = Column(Integer)

    def __eq__(self, other):
        if not isinstance(other, MyItem):
            return False
        return other.id == self.id and other.name == self.name and other.age == self.age

def test_add():
    engine = create_engine('sqlite://')
    repo = SQLRepo(MyItem, engine=engine)
    repo.create()

    repo.add(MyItem(id="a", name="Jack", age=20))
    assert repo.session.query(MyItem).all() == [MyItem(id="a", name="Jack", age=20)]
    repo.add(MyItem(id="b", name="John", age=30))
    assert repo.session.query(MyItem).all() == [
        MyItem(id="a", name="Jack", age=20),
        MyItem(id="b", name="John", age=30),
    ]

def test_filter_by():
    engine = create_engine('sqlite://')
    repo = SQLRepo(MyItem, engine=engine)
    repo.create()

    items = [
        MyItem(id="a", name="Jack", age=20),
        MyItem(id="b", name="John", age=30),
        MyItem(id="c", name="James", age=30),
        MyItem(id="d", name="Johnny", age=30),
        MyItem(id="e", name="Jesse", age=40),
    ]
    for item in items:
        repo.session.add(item)
    repo.session.commit()

    assert repo.filter_by(age=30).first() == MyItem(id="b", name="John", age=30)
    assert repo.filter_by(age=30).last() == MyItem(id="d", name="Johnny", age=30)
    assert repo.filter_by(age=30).all() == [
        MyItem(id="b", name="John", age=30),
        MyItem(id="c", name="James", age=30),
        MyItem(id="d", name="Johnny", age=30),
    ]

    assert repo.filter_by(age=30).limit(2) == [
        MyItem(id="b", name="John", age=30),
        MyItem(id="c", name="James", age=30),
    ]