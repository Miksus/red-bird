
from pydantic import BaseModel
import pytest
from redbird import BaseRepo, BaseResult
from redbird.repos import MemoryRepo

def test_id_field():
    class MyModel(BaseModel):
        __id_field__ = "my_id"
        my_id: str
        id: str
    repo = MemoryRepo(model=MyModel)
    assert repo.id_field == "my_id"

    repo = MemoryRepo(model=MyModel, id_field="id")
    assert repo.id_field == "id"

    repo = MemoryRepo(id_field="id")
    assert repo.id_field == "id"

    repo = MemoryRepo()
    assert repo.id_field is None

def test_result_incomplete():
    with pytest.raises(TypeError) as excinfo:
        class MyResults(BaseResult):
            ...
        MyResults()
    assert str(excinfo.value.args[0]) == "Can't instantiate abstract class MyResults with abstract methods delete, query_data, update"

def test_repo_incomplete():
    with pytest.raises(TypeError) as excinfo:
        class MyRepo(BaseRepo):
            ...
        MyRepo()
    assert str(excinfo.value.args[0]) in (
        # Python <3.10
        "Can't instantiate abstract class MyRepo with abstract methods insert",
        # Python >=3.10
        "Can't instantiate abstract class MyRepo with abstract method insert",
    )
