
from textwrap import dedent
from typing import Optional

import pytest

from redbird.repos import JSONDirectoryRepo
from pydantic import BaseModel

class Item(BaseModel):
    id: str
    name: str
    age: Optional[int]

def sort_items(items, repo, field="id"):
    return list(sorted(items, key=lambda x: repo.get_field_value(x, field)))

def test_missing_id(tmpdir):
    with pytest.raises(ValueError):
        repo = JSONDirectoryRepo(path=tmpdir, model=Item)

def test_filecontent(tmpdir):
    repo = JSONDirectoryRepo(path=tmpdir, model=Item, id_field="id")

    repo.add(Item(id="1111", name="Jack", age=20))
    repo.add(Item(id="2222", name="John"))
    repo.add(Item(id="3333", name="James", age=30))

    content = (tmpdir / "1111.json").read_text(encoding="UTF-8")
    assert content == '{"id": "1111", "name": "Jack", "age": 20}'

    repo.filter_by(id="1111").update(age=40)
    content = (tmpdir / "1111.json").read_text(encoding="UTF-8")
    assert content == '{"id": "1111", "name": "Jack", "age": 40}'

    repo.filter_by(id="2222").delete()
    assert not (tmpdir / "2222.json").exists()

    repo.filter_by().delete()
    assert not (tmpdir / "1111.json").exists()
    assert not (tmpdir / "3333.json").exists()


def test_kwds_json(tmpdir):
    repo = JSONDirectoryRepo(path=tmpdir, model=Item, id_field="id", kwds_json_dump={'indent': 4, 'sort_keys': True})

    repo.add(Item(id="1111", name="Jack", age=20))

    content = (tmpdir / "1111.json").read_text(encoding="UTF-8")
    assert content == dedent('''
    {
        "age": 20,
        "id": "1111",
        "name": "Jack"
    }
    ''')[1:-1]

    # Testing reading
    assert repo['1111'] == Item(id="1111", name="Jack", age=20)

def test_dict_operations(tmpdir):
    repo = JSONDirectoryRepo(path=tmpdir, id_field="id")

    # Insert
    repo.add(dict(id="a", name="Jack", age=20))
    repo.add(dict(id="b", name="Jack"))
    repo.add(dict(id="c", name="James", age=30))

    # Read
    actual = repo.filter_by().all()
    expected = [
        dict(id="a", name="Jack", age=20),
        dict(id="b", name="Jack"),
        dict(id="c", name="James", age=30)
    ]
    assert sort_items(actual, repo) == sort_items(expected, repo)

    actual = repo.filter_by(name="Jack").all() 
    expected = [
        dict(id="a", name="Jack", age=20),
        dict(id="b", name="Jack")
    ]
    assert sort_items(actual, repo) == sort_items(expected, repo)

    actual = repo.filter_by(age=30).all() 
    expected = [
        dict(id="c", name="James", age=30),
    ]
    assert sort_items(actual, repo) == sort_items(expected, repo)

    assert repo.filter_by(age="30").all() == []

    # Update
    repo.filter_by(name="Jack").update(age=50)
    actual = repo.filter_by().all()
    expected = [
        dict(id="a", name="Jack", age=50),
        dict(id="b", name="Jack", age=50),
        dict(id="c", name="James", age=30)
    ]
    assert sort_items(actual, repo) == sort_items(expected, repo)

    # Delete
    repo.filter_by(name="Jack").delete()
    assert repo.filter_by().all() == [
        #dict(id="a", name="Jack", age=50),
        #dict(id="b", name="Jack", age=50),
        dict(id="c", name="James", age=30)
    ]