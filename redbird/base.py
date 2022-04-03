
from abc import abstractmethod, ABC
from ctypes import Union
from operator import getitem, setitem
from typing import Any, Dict, Generator, List, Mapping, Tuple
from dataclasses import dataclass

from pydantic import BaseModel

from redbird.exc import KeyFoundError

from .oper import Operation

class DummySession:
    ...

class BaseResult(ABC):
    """Abstract query result

    Result classes handle the querying to the
    repository providing convenient way to 
    stack operations and reuse queries.
    
    Subclass of BaseRepo should also have custom
    subclass of BaseResult as cls_result attribute.
    """

    query_: dict
    repo: 'BaseRepo'

    def __init__(self, query:dict=None, repo:'BaseRepo'=None):
        self.repo = repo
        self.query_ = self.format_query(query)
        
    def first(self):
        "Return first item"
        for item in self.query():
            return item
    
    def last(self):
        "Return last item"
        item = None
        for item in self.query():
            pass
        return item

    def all(self):
        "Return all items"
        return list(self.query())

    def limit(self, n:int) -> Generator:
        "Return n items"
        items = []
        for i, item in enumerate(self.query(), start=1):
            if i > n:
                break
            items.append(item)
        return items
        
    @abstractmethod
    def query(self) -> Generator:
        "Get actual result"
        ...

    def __iter__(self):
        return self.query()

    @abstractmethod
    def update(self, **kwargs):
        "Update the resulted items"

    @abstractmethod
    def delete(self):
        "Delete the resulted items"

    def count(self):
        "Count the resulted items"
        return len(list(self))


    def format_query(self, query:dict) -> dict:
        "Turn the query to a form that's understandable by the underlying database"
        for field_name, oper_or_value in query.copy().items():
            if isinstance(oper_or_value, Operation):
                query[field_name] = self.format_operation(oper_or_value)
        return query

    def format_operation(self, oper:Operation):
        result_format_method = oper._get_formatter(self)
        return result_format_method(oper)

class BaseRepo(ABC):
    """Abstract Repository

    Base class for the repository pattern.

    """

    default_id_field: str = "id"
    id_field: str
    model = dict
    cls_result: BaseResult

    def __init__(self, model=None, id_field=None, field_access:str=None):
        self.model = dict if model is None else model
        self.id_field = id_field or self.default_id_field
        
        if field_access is None:
            field_access = "item" if self.model == dict else "attr"
        if field_access not in ("item", "attr"):
            raise ValueError("Only 'item' and 'attr' are possible ways to access item's values.")
        self.field_access = field_access

    def __iter__(self):
        "Iterate over the repository"
        return iter(self.filter_by().all())

    def __getitem__(self, id):
        "Get item from the repository using ID"
        qry = {self.id_field: id}
        item = self.filter_by(**qry).first()
        if item is None:
            raise KeyError(f"Item {id} not found.")
        return item

    def __delitem__(self, id):
        "Delete item from the repository using ID"
        qry = {self.id_field: id}
        rows = self.filter_by(**qry).delete()
        if isinstance(rows, int):
            # Returned value is number of rows deleted
            if rows == 0:
                raise KeyError(f"Index {id} not found.")

    def __setitem__(self, id, attrs:dict):
        "Update given item"
        qry = {self.id_field: id}
        self.filter_by(**qry).update(**attrs)

# Item based
    def add(self, item, if_exists="raise"):
        "Add an item to the repository"
        item = self.to_item(item)
        if if_exists == "raise":
            self.insert(item)
        elif if_exists == "update":
            self.upsert(item)
        elif if_exists == "ignore":
            try:
                self.insert(item)
            except KeyFoundError:
                pass
        else:
            raise ValueError(f"Invalid value: {if_exists}")

    @abstractmethod
    def insert(self):
        "Add an item to the repository"
        ...

    def upsert(self, item):
        try:
            self.insert(item)
        except KeyFoundError:
            self.update(item)

    def delete(self, item):
        id_ = self.get_field_value(item, self.id_field)
        del self[id_]

    def update(self, item):
        "Update an item in the repository"
        qry = {self.id_field: self.get_field_value(item, self.id_field)}
        values = self.item_to_dict(item)
        # We don't update the ID
        values.pop(self.id_field)
        self.filter_by(**qry).update(**values)

    def replace(self, item):
        "Update an item in the repository"
        self.delete(item)
        self.add(item)

    def item_to_dict(self, item) -> dict:
        if isinstance(item, dict):
            return item
        elif isinstance(item, BaseModel):
            # Pydantic model
            return item.dict(exclude_unset=True)
        else:
            return dict(**item)

# Keyword arguments
    def filter_by(self, **kwargs) -> BaseResult:
        "Get items from the repository by filtering using keyword args"
        return self.cls_result(query=kwargs, repo=self)

    def data_to_item(self, data:Mapping):
        "Turn object from repo (row, doc, dict, etc.) to item"
        return self.model(**data)

    def to_item(self, obj):
        "Turn an object to item"
        if isinstance(obj, self.model):
            return obj
        elif isinstance(obj, dict):
            return self.model(**obj)
        elif isinstance(obj, (list, tuple, set)):
            return self.model(*obj)
        else:
            raise TypeError(f"Cannot cast {type(obj)} to {self.model}")

    def get_field_value(self, item, key):
        """Utility method to get key's value from an item
        
        If item's fields are accessed via attribute,
        getattr is used. If fields are accessed via 
        items, getitem is used.
        """
        func = {
            "attr": getattr,
            "item": getitem,
        }[self.field_access]
        
        return func(item, key)

    def set_field_value(self, item, key, value):
        """Utility method to set field's value in an item
        
        If item's fields are accessed via attribute,
        setattr is used. If fields are accessed via 
        items, setitem is used.
        """
        func = {
            "attr": setattr,
            "item": setitem,
        }[self.field_access]
        
        func(item, key, value)