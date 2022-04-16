
from abc import abstractmethod, ABC
from operator import getitem, setitem
from textwrap import dedent, indent, shorten
from typing import Any, Dict, Generator, Iterator, List, Literal, Mapping, Tuple, Type, TypeVar, Union
from dataclasses import dataclass
import warnings

from pydantic import BaseModel

from redbird.exc import ConversionWarning, DataToItemError, KeyFoundError, ItemToDataError
from redbird.utils.case import to_case

from .oper import Operation

Item = TypeVar("Item")
Data = TypeVar("Data")

class BasicQuery(BaseModel, extra="allow"):
    ...

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
        
    def query(self) -> Iterator[Item]:
        "Get actual result"
        for data in self.query_data():
            try:
                yield self.repo.data_to_item(data)
            except ValueError:
                if self.repo.errors_query == "raise":
                    raise
                elif self.repo.errors_query == "warn":
                    warnings.warn(f'Converting data to item failed: \n{data}', ConversionWarning)

    @abstractmethod
    def query_data(self) -> Iterator[Data]:
        "Get actual result"
        ...

    def validate(self, max_shown=10):
        tmpl = dedent("""
            Validation Errors
            =================
            Model: {model}
            Errors: {n_errors}

            Details
            =================
            {details}
            """)[1:]
        errors = []
        for data in self.query_data():
            try:
                self.repo.data_to_item(data)
            except ValueError as exc:
                errors.append((data, exc))
        if errors:
            msg_details = ""
            for data, exc in errors[:max_shown]:
                item = shorten(str(data), width=100)
                msg_details = msg_details + dedent("""
                    Item
                    -----------------
                    {item}

                    {exc}
                    """).format(item=item, exc=indent(str(exc), " " * 2))
            if len(errors) > max_shown:
                msg_details = msg_details + "\n[...]"

            msg_details = indent(msg_details, " " * 2)

            raise ValueError(tmpl.format(n_errors=len(errors), details=msg_details, model=self.repo.model))

    def __iter__(self) -> Iterator[Item]:
        return self.query()

    @abstractmethod
    def update(self, **kwargs):
        "Update the resulted items"

    @abstractmethod
    def delete(self):
        "Delete the resulted items"

    def count(self) -> int:
        "Count the resulted items"
        return len(list(self))


    def format_query(self, query:dict) -> dict:
        "Turn the query to a form that's understandable by the underlying database"
        qry = self.repo.query_format(**query)
        return qry.format(self.repo) if hasattr(qry, "format") else qry.dict()

    def format_query_value(self, oper_or_value:Union[Operation, Any]):
        "Turn an operation to string/object understandable by the underlying database"
        if isinstance(oper_or_value, Operation):
            oper = oper_or_value
            result_format_method = oper._get_formatter(self)
            value = result_format_method(oper)
        else:
            value = oper_or_value
        return value

    def format_query_field(self, key:str, value:Union[Operation, Any]) -> str:
        "Turn a query key to a field understandable by the underlying database"
        conf = self.repo.query_format
        field_case = getattr(conf, "__case__", None)
        if field_case is not None:
            key = to_case(key, case=field_case)
        return key

class BaseRepo(ABC):
    """Abstract Repository

    Base class for the repository pattern.

    """

    default_id_field: str = "id"
    id_field: str
    model = dict
    cls_result: Type[BaseResult]
    query_format: Type[BaseModel]
    default_query_format: Type[BaseModel] = BasicQuery

    errors_query: Literal['raise', 'warn', 'discard']

    def __init__(self, model=None, id_field=None, field_access:str=None, query:BaseModel=None, errors_query="raise"):
        self.model = dict if model is None else model
        self.id_field = id_field or self.default_id_field
        
        self.field_access = field_access
        self.query_format = self.default_query_format if query is None else query

        self.errors_query = errors_query

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
        "Add an item to the repository"
        ...

    def upsert(self, item: Item):
        try:
            self.insert(item)
        except KeyFoundError:
            self.update(item)

    def delete(self, item: Item):
        id_ = self.get_field_value(item, self.id_field)
        del self[id_]

    def update(self, item: Item):
        "Update an item in the repository"
        qry = {self.id_field: self.get_field_value(item, self.id_field)}
        values = self.item_to_dict(item)
        # We don't update the ID
        values.pop(self.id_field)
        self.filter_by(**qry).update(**values)

    def replace(self, item: Item):
        "Update an item in the repository"
        self.delete(item)
        self.add(item)

    def item_to_dict(self, item: Item) -> dict:
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

    def data_to_item(self, data:Data) -> Item:
        "Turn object from repo (row, doc, dict, etc.) to item"
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
        func = {
            "attr": getattr,
            "item": getitem,
        }[self.field_access]
        
        return func(item, key)

    def set_field_value(self, item: Item, key, value):
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

    @property
    def field_access(self) -> Literal['item', 'attr']:
        return self._field_access
    
    @field_access.setter
    def field_access(self, value: Literal['item', 'attr', None]):
        if value is None:
            value = 'item' if self.model == dict else 'attr'
        if value not in ('item', 'attr'):
            raise ValueError("Only 'item' and 'attr' are possible ways to access item's values.")
        self._field_access = value
