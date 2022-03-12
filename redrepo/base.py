
from abc import abstractmethod, ABC
from ctypes import Union
from typing import Any, Generator, List
from dataclasses import dataclass

class BaseResult(ABC):
    """Result of a filter"""

    cursor: Any

    def __init__(self, query=None, repo:'Repo'=None):
        self.repo = repo
        self.query_ = query
        
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

    def parse_item(self, item):
        "Turn object from repo (row, doc, etc.) to item"
        return self.repo.cls_item(**item)

    @abstractmethod
    def update(self, **kwargs):
        "Update the resulted items"

    @abstractmethod
    def delete(self):
        "Delete the resulted items"

    def count(self):
        "Count the resulted items"
        return len(list(self))


class BaseRepo(ABC):

    """Item Repo

    Examples
    --------
    Creating a repo:

    .. code-block:: python

        class Companies(Repo):
            pass

        comp_repo = Companies(session)

    Getting items:

    .. code-block:: python
        # Get an item based on ID
        comp_repo["123456-1"]
    
        # Get items
        comp_repo.filter_by(exchange="XHEL")

    Setting items:

    .. code-block:: python

        comp_repo.add({"id": "12345-2", "name": "Company Ltd"})

    Deleting item:

    .. code-block:: python

        del comp_repo['12345-2']

        # Or multiple
        comp_repo.delete_by(exchange="XHEL")
    """

    default_id_field: str = "id"
    cls_item = dict
    cls_result: BaseResult


    def __iter__(self):
        "Iterate over the repository"
        return self.filter_by()

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
        rows = self.delete_by(**qry)
        if isinstance(rows, int):
            # Returned value is number of rows deleted
            if rows == 0:
                raise KeyError(f"Index {id} not found.")

    def __setitem__(self, id, attrs:dict):
        "Update given item"
        qry = {self.id_field: id}
        self.update_by(qry, attrs)

# Item based
    @abstractmethod
    def add(self, item):
        "Add an item to the repository"
        ...

    def delete(self, item):
        id_ = getattr(item, self.id_field)
        del self[id_]

    @abstractmethod
    def update(self, item):
        "Update an item in the repository"
        ...

    def format_item(self, item):
        return self.cls_item(**item)

# Keyword arguments
    def filter_by(self, **kwargs) -> BaseResult:
        "Get items from the repository by filtering using keyword args"
        return self.cls_result(query=kwargs, repo=self)

    @abstractmethod
    def delete_by(self, **kwargs):
        "Delete items from the repository by filtering using keyword args"
        ...
