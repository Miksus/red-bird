
from operator import getitem
from typing import Any, Dict, List

from pydantic import BaseModel, PrivateAttr
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
    model : Type
        Class of an item in the repository.
        Commonly dict or subclass of Pydantic
        BaseModel. By default dict
    id_field : str, optional
        Attribute or key that identifies each item
        in the repository.
    field_access : {'attr', 'key'}, optional
        How to access a field in an item. Either
        by attribute ('attr') or key ('item').
        By default guessed from the model.
    query : Type, optional
        Query model of the repository.
    errors_query : {'raise', 'warn', 'discard'}
        Whether to raise an exception, warn or discard
        the item in case of validation error in 
        converting data to the item model from
        the repository. By default raise 
    collection : list
        The collection.
    
    Examples
    --------
    .. code-block:: python

        repo = MemoryRepo()

    .. code-block:: python

        repo = MemoryRepo(collection=[
            {"car_type": "van", "color": "red"},
            {"car_type": "truck", "color": "red"}
        ])
    """
    #cls_result = MemoryResult
    collection: List[Any] = []

    _session = PrivateAttr()
    def insert(self, item):
        if self.id_field is not None:
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

    @property
    def session(self):
        if not hasattr(self, "_session"):
            self._session = DummySession()
        return self._session