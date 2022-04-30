
from operator import getitem
from typing import Dict, List

from pydantic import BaseModel
from redbird import BaseRepo, BaseResult
from redbird.templates import TemplateRepo
from redbird.exc import KeyFoundError
from redbird.oper import Operation
from redbird.dummy import DummySession


class QueryMatcher:

    def __init__(self, query, value_getter):
        self.query = query
        self.value_getter = value_getter

    def __contains__(self, item):
        "Match whether item fulfills the query"
        get_value = self.value_getter
        return all(
            val.evaluate(get_value(item, key)) if isinstance(val, Operation) else get_value(item, key) == val
            for key, val in self.query.items()
        )

class MemoryRepo(TemplateRepo):
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
    #cls_result = MemoryResult
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

    def query_read(self, query):
        query = QueryMatcher(query, value_getter=self.get_field_value)
        col = self.collection
        for data in col:
            if data in query:
                yield data

    def query_update(self, query:dict, values:dict):
        query = QueryMatcher(query, value_getter=self.get_field_value)
        col = self.collection
        for item in col:
            if item in query:
                for key, val in values.items():
                    self.set_field_value(item, key, val)

    def query_delete(self, query:dict):
        query = QueryMatcher(query, value_getter=self.get_field_value)
        cont = self.collection
        self.collection = [
            item 
            for item in cont 
            if not item in query
        ]
