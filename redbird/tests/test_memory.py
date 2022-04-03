import pytest
from redbird.repos.memory import MemoryRepo
from pydantic import BaseModel


class MyItem(BaseModel):
    id: str
    name: str
    age: int


def test_add():
    repo = MemoryRepo(MyItem)
    assert repo.store == []

    repo.add(MyItem(id="a", name="Jack", age=20))
    assert repo.store == [MyItem(id="a", name="Jack", age=20)]
    repo.add(MyItem(id="b", name="John", age=30))
    assert repo.store == [
        MyItem(id="a", name="Jack", age=20),
        MyItem(id="b", name="John", age=30),
    ]

def test_filter_by():
    repo = MemoryRepo(MyItem, store=[
        MyItem(id="a", name="Jack", age=20),
        MyItem(id="b", name="John", age=30),
        MyItem(id="c", name="James", age=30),
        MyItem(id="d", name="Johnny", age=30),
        MyItem(id="e", name="Jesse", age=40),
    ])

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

def test_getitem():
    repo = MemoryRepo(MyItem, store=[
        MyItem(id="a", name="Jack", age=20),
        MyItem(id="b", name="John", age=30),
        MyItem(id="c", name="James", age=30),
        MyItem(id="d", name="Johnny", age=30),
        MyItem(id="e", name="Jesse", age=40),
    ])
    assert repo["b"] == MyItem(id="b", name="John", age=30)
    with pytest.raises(KeyError):
        repo["non_existing"]