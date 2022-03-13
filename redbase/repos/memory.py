
from typing import Dict, List

from pydantic import BaseModel
from redbase import BaseRepo, BaseResult
from redbase.exc import KeyFoundError
from redbase.oper import Operation

class DummySession:
    """Dummy session

    Imitates similar session objects as SQLAlchemy's
    session in order to avoid code changes if
    in-memory repository is used.
    """

    def close(self):
        ...
    
    def remove(self):
        ...

class MemoryResult(BaseResult):

    repo: 'MemoryRepo'

    def query(self):
        l = self.repo.collection
        for item in l:
            if self._match(item):
                yield item

    def update(self, **kwargs):
        for item in self.query():
            for key, val in kwargs.items():
                setattr(item, key, val)

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
        return all(
            val.evaluate(getattr(item, key)) if isinstance(val, Operation) else getattr(item, key) == val
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

    def __init__(self, model:BaseModel, collection=None, id_field=None):
        self.model = model
        self.collection = [] if collection is None else collection
        self.id_field = id_field or self.default_id_field
        self.session = DummySession()

    def delete_by(self, **kwargs):
        old_count = len(self.collection)
        self.collection = [item for item in self.collection if not self._match_kwargs(item, kwargs)]
        new_count = len(self.collection)
        return old_count - new_count

    def insert(self, item):
        id_ = getattr(item, self.id_field)
        if id_ in [getattr(col_item, self.id_field) for col_item in self.collection]:
            raise KeyFoundError(f"Item {id_} already in the collection.")
        self.collection.append(item)

    def _match_kwargs(self, item, kwargs):
        return all(
            getattr(item, key) == val
            for key, val in kwargs.items()
        )
