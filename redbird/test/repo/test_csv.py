
from typing import Optional

from redbird.repos import CSVFileRepo
from pydantic import BaseModel

class Item(BaseModel):
    id: str
    name: str
    age: Optional[int]

def test_filecontent(tmpdir):
    file = tmpdir / "test.csv"
    repo = CSVFileRepo(filename=file, model=Item)

    repo.add(Item(id="a", name="Jack", age=20))
    repo.add(Item(id="b", name="John"))
    repo.add(Item(id="c", name="James", age=30))

    assert "id,name,age\na,Jack,20\nb,John,\nc,James,30\n" == file.read_text(encoding="UTF-8")

    repo.filter_by(id="b").update(age=40)
    assert "id,name,age\na,Jack,20\nb,John,40\nc,James,30\n" == file.read_text(encoding="UTF-8")

    repo.filter_by(id="b").delete()
    assert "id,name,age\na,Jack,20\nc,James,30\n" == file.read_text(encoding="UTF-8")

    repo.filter_by().delete()
    assert "id,name,age\n" == file.read_text(encoding="UTF-8")

def test_none(tmpdir):
    file = tmpdir / "test.csv"
    repo = CSVFileRepo(filename=file, model=Item)
    repo.add(Item(id="b", name="John"))
    assert [Item(id="b", name="John", age=None)] == repo.filter_by().all()

def test_none_read(tmpdir):
    file = tmpdir / "test.csv"
    repo = CSVFileRepo(filename=file, model=Item)

    assert [] == repo.filter_by().all()
    assert repo.filename.read_text() == "id,name,age\n"

def test_fieldnames_read(tmpdir):
    file = tmpdir / "test.csv"
    repo = CSVFileRepo(filename=file, fieldnames=["id", "name", "age"])

    assert [] == repo.filter_by().all()
    assert repo.filename.read_text() == "id,name,age\n"

def test_kwds_csv(tmpdir):
    file = tmpdir / "test.csv"
    repo = CSVFileRepo(filename=file, model=Item, kwds_csv={'delimiter': '|'})

    repo.add(Item(id="a", name="Jack", age=20))
    repo.add(Item(id="b", name="John"))

    assert "id|name|age\na|Jack|20\nb|John|\n" == file.read_text(encoding="UTF-8")

    assert repo.filter_by().all() == [Item(id="a", name="Jack", age=20), Item(id="b", name="John")]

def test_dict_operations(tmpdir):
    file = tmpdir / "test.csv"
    repo = CSVFileRepo(filename=file, fieldnames=['id', 'name', 'age'])

    # Insert
    repo.add(dict(id="a", name="Jack", age=20))
    repo.add(dict(id="b", name="Jack"))
    repo.add(dict(id="c", name="James", age=30))

    # Read
    assert repo.filter_by().all() == [
        dict(id="a", name="Jack", age="20"),
        dict(id="b", name="Jack", age=None),
        dict(id="c", name="James", age="30")
    ]
    assert repo.filter_by(name="Jack").all() == [
        dict(id="a", name="Jack", age="20"),
        dict(id="b", name="Jack", age=None)
    ]
    assert repo.filter_by(age="30").all() == [
        dict(id="c", name="James", age="30"),
    ]
    assert repo.filter_by(age=30).all() == []

    # Update
    repo.filter_by(name="Jack").update(age=50)
    assert repo.filter_by().all() == [
        dict(id="a", name="Jack", age="50"),
        dict(id="b", name="Jack", age="50"),
        dict(id="c", name="James", age="30")
    ]

    # Delete
    repo.filter_by(name="Jack").delete()
    assert repo.filter_by().all() == [
        #dict(id="a", name="Jack", age="50"),
        #dict(id="b", name="Jack", age="50"),
        dict(id="c", name="James", age="30")
    ]