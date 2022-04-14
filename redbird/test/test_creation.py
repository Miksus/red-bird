
import pytest
from redbird import BaseRepo, BaseResult

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
    assert str(excinfo.value.args[0]) == "Can't instantiate abstract class MyRepo with abstract methods insert"
