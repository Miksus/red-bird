
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

