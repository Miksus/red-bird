
from abc import abstractmethod, ABC
from operator import getitem, setitem
from textwrap import dedent, indent, shorten
from typing import Any, ClassVar, Dict, Generator, Iterator, List, Mapping, Optional, Tuple, Type, TypeVar, Union
from dataclasses import dataclass
import warnings

from pydantic import BaseModel, Field, validator

from redbird.exc import ConversionWarning, DataToItemError, KeyFoundError, ItemToDataError, _handle_conversion_error
from redbird.utils.case import to_case

from .oper import Operation

try:
    from typing import Literal
except ImportError: # pragma: no cover
    from typing_extensions import Literal


Item = TypeVar("Item")
Data = TypeVar("Data")

class BasicQuery(BaseModel, extra="allow"):
    ...

class DummySession:
    ...

class BaseResult(ABC):
    """Abstract filter result

    Result classes add additional alchemy
    to Red Bird providing convenient ways
    to read, modify or delete data. 
    
    Subclass of BaseRepo should also have custom
    subclass of BaseResult as cls_result attribute.
    """

    query_: dict
    repo: 'BaseRepo'

    def __init__(self, query:dict=None, repo:'BaseRepo'=None):
        self.repo = repo
        self.query_ = self.format_query(query)
        
    def first(self) -> Item:
        "Return first item"
        for item in self.query():
            return item
    
    def last(self) -> Item:
        "Return last item"
        item = None
        for item in self.query():
            pass
        return item

    def all(self) -> List[Item]:
        "Return all items"
        return list(self.query())

    def limit(self, n:int) -> List[Item]:
        "Return n items"
        items = []
        for i, item in enumerate(self.query(), start=1):
            if i > n:
                break
            items.append(item)
        return items
        
    def query(self) -> Iterator[Item]:
        "Get actual result"
        for data in self.query_data():
            try:
                yield self.repo.data_to_item(data)
            except ValueError:
                _handle_conversion_error(self.repo, data)

    @abstractmethod
    def query_data(self) -> Iterator[Data]:
        "Get actual result"
        ...

    def __iter__(self) -> Iterator[Item]:
        return self.query()

    @abstractmethod
    def update(self, **kwargs):
        "Update the resulted items"

    @abstractmethod
    def delete(self):
        "Delete the resulted items"

    def replace(self, __values:dict=None, **kwargs):
        "Replace the existing item(s) with given"
        if __values is not None:
            kwargs.update(__values)
        if self.count() > 1:
            raise KeyError("You may only replace one item.")
        self.delete()
        self.repo.add(kwargs)

    def count(self) -> int:
        "Count the resulted items"
        return len(list(self))

    def format_query(self, query:dict) -> dict:
        "Turn the query to a form that's understandable by the underlying database"
        qry = self.repo.query_model(**query)
        return qry.format(self.repo) if hasattr(qry, "format") else qry.dict()

class BaseRepo(ABC, BaseModel):
    """Abstract Repository

    Base class for the repository pattern.

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
    """
    cls_result: ClassVar[Type[BaseResult]]

    model: Type = dict
    id_field: Optional[str]
    
    query_model: Optional[Type[BaseModel]] = BasicQuery

    errors_query: Literal['raise', 'warn', 'discard'] = 'raise'
    field_access: Literal['attr', 'key', 'infer'] = 'infer'

    # Attributes that specifies how the repo behaves
    ordered: bool = Field(default=False, const=True)

    @validator('id_field', always=True)
    def set_id_field(cls, value, values):
        if value is None:
            # Get id_field from model
            mdl = values.get("model")
            return getattr(mdl, "__id_field__", None)
        return value

    def __iter__(self) -> Iterator[Item]:
        "Iterate over the repository"
        return iter(self.filter_by().all())

    def __getitem__(self, id) -> Item:
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
    def add(self, item: Item, if_exists="raise"):
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
        """Insert item to the repository
        
        Parameters
        ----------
        item: instance of model
            Item to insert to the repository

        Examples
        --------
        .. code-block:: python

            repo.insert(Item(id="a", color="red"))
        """
        ...

    def upsert(self, item: Item):
        """Upsert item to the repository

        Upsert is an insert if the item
        does not exist in the repository
        and update if it does.
        
        Parameters
        ----------
        item: instance of model
            Item to upsert to the repository

        Examples
        --------
        .. code-block:: python

            repo.upsert(Item(id="a", color="red"))
        """
        try:
            self.insert(item)
        except KeyFoundError:
            self.update(item)

    def delete(self, item: Item):
        """Delete item from the repository
        
        Parameters
        ----------
        item: instance of model
            Item to delete from the repository

        Examples
        --------
        .. code-block:: python

            repo.delete(Item(id="a", color="red"))
        """
        id_ = self.get_field_value(item, self.id_field)
        del self[id_]

    def update(self, item: Item):
        """Update item in the repository
        
        Parameters
        ----------
        item: instance of model
            Item to update in the repository

        Examples
        --------
        .. code-block:: python

            repo.update(Item(id="a", color="red"))
        """
        qry = {self.id_field: self.get_field_value(item, self.id_field)}
        values = self.item_to_dict(item, exclude_unset=True)
        # We don't update the ID
        values.pop(self.id_field, None)
        self.filter_by(**qry).update(**values)

    def replace(self, item: Item):
        "Update an item in the repository"
        self.delete(item)
        self.add(item)

    def item_to_dict(self, item: Item, exclude_unset=True) -> dict:
        if isinstance(item, dict):
            return item
        elif isinstance(item, BaseModel):
            # Pydantic model
            return item.dict(exclude_unset=exclude_unset)
        else:
            return dict(**item)

# Keyword arguments
    
    def get_by(self, id):
        "Get item based on ID but returns result for further operations"
        qry = {self.id_field: id}
        return self.filter_by(**qry)

    def filter_by(self, **kwargs) -> BaseResult:
        """Filter the repository
        
        Parameters
        ----------
        **kwargs : dict
            Query which is used to conduct 
            furher operation.

        Examples
        --------
        .. code-block:: python

            repo.filter_by(color="red")
        """
        return self.cls_result(query=kwargs, repo=self)

    def data_to_item(self, data:Data) -> Item:
        "Turn object from repo (row, doc, dict, etc.) to item"
        if isinstance(data, self.model):
            # Already the right type
            return data
        if not isinstance(data, Mapping):
            # data is namespace-like
            data = vars(data)
        try:
            return self.model(**data)
        except Exception as exc:
            raise DataToItemError(f"Could not transform {data}") from exc

    def to_item(self, obj) -> Item:
        "Turn an object to item"
        if isinstance(obj, self.model):
            return obj
        elif isinstance(obj, dict):
            return self.model(**obj)
        elif isinstance(obj, (list, tuple, set)):
            return self.model(*obj)
        else:
            raise TypeError(f"Cannot cast {type(obj)} to {self.model}")

    def get_field_value(self, item: Item, key):
        """Utility method to get key's value from an item
        
        If item's fields are accessed via attribute,
        getattr is used. If fields are accessed via 
        items, getitem is used.
        """
        if self.field_access == "infer":
            field_access = "key" if hasattr(self.model, "__getitem__") else "attr"
        else:
            field_access = self.field_access
        func = {
            "attr": getattr,
            "key": getitem,
        }[field_access]
        
        return func(item, key)

    def set_field_value(self, item: Item, key, value):
        """Utility method to set field's value in an item
        
        If item's fields are accessed via attribute,
        setattr is used. If fields are accessed via 
        items, setitem is used.
        """
        if self.field_access == "infer":
            field_access = "key" if hasattr(self.model, "__getitem__") else "attr"
        else:
            field_access = self.field_access
        func = {
            "attr": setattr,
            "key": setitem,
        }[field_access]
        
        func(item, key, value)
