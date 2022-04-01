
from typing import Optional

from redbase.repos import MongoRepo

import pytest
from pydantic import BaseModel

class Item(BaseModel):
    id: str
    name: str
    age: Optional[int]


class ItemWithCol(BaseModel):
    __colname__ = 'items'
    id: str
    name: str
    age: Optional[int]

def test_creation_defaults(mongo_uri):
    repo = MongoRepo(ItemWithCol, url=mongo_uri, id_field="id")

    # Empty the collection
    pytest.importorskip("pymongo")

    col = repo.get_collection()
    assert col.name == "items"
    assert col.database.name == "pytest"

def test_creation_passed(mongo_uri):
    repo = MongoRepo(Item, url=mongo_uri, database="pytest2", collection="items", id_field="id")

    # Empty the collection
    pytest.importorskip("pymongo")

    col = repo.get_collection()
    assert col.name == "items"
    assert col.database.name == "pytest2"
