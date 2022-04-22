
from operator import getitem
from typing import Dict, List

from pydantic import BaseModel
from redbird import BaseRepo, BaseResult
from redbird.exc import KeyFoundError
from redbird.oper import Operation
from redbird.dummy import DummySession

class MemoryResult(BaseResult):

    repo: 'MemoryRepo'

    def query_data(self):
        col = self.repo.collection
        for data in col:
            if self._match(data):
                yield data

    def update(self, __values:dict=None, **kwargs):
        if __values is not None:
            kwargs.update(__values)
        col = self.repo.collection
        for item in col:
            if self._match(item):
                for key, val in kwargs.items():
                    self.repo.set_field_value(item, key, val)

    def delete(self, **kwargs):
        cont = self.repo.collection
        self.repo.collection = [
            item 
            for item in cont 
            if not self._match(item)
        ]

    def format_query(self, query:dict):
        return query

    def _match(self, item):
        "Match whether item fulfills the query"
        get_value = self.repo.get_field_value
        return all(
            val.evaluate(get_value(item, key)) if isinstance(val, Operation) else get_value(item, key) == val
            for key, val in self.query_.items()
        )

class MemoryRepo(BaseRepo):
    """Memory repository

    This is a repository which operates in memory.
    Useful for unit tests and prototyping.

    Parameters
    ----------
    model : BaseModel
        Class of an item in the repository.
    collection : list
        The collection.
    """
    cls_result = MemoryResult
    collection: List[BaseModel]

    def __init__(self, *args, collection=None, **kwargs):
        super().__init__(*args, **kwargs)
        self.collection = [] if collection is None else collection
        self.session = DummySession()

    def insert(self, item):
        id_ = self.get_field_value(item, self.id_field)
        if id_ in [self.get_field_value(col_item, self.id_field) for col_item in self.collection]:
            raise KeyFoundError(f"Item {id_} already in the collection.")
        data = self.item_to_data(item)
        self.collection.append(data)

    def item_to_data(self, item):
        return item