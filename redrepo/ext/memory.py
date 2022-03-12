
from typing import List
from redrepo import BaseRepo, BaseResult
from redrepo.operation import Operation

class ListResult(BaseResult):

    store: List
    repo: 'ListRepo'

    def query(self):
        l = self.repo.store
        for item in l:
            if self._match(item):
                yield item

    def update(self, **kwargs):
        for item in self.query():
            for key, val in kwargs.items():
                setattr(item, key, val)

    def delete(self, **kwargs):
        cont = self.repo.store
        self.repo.store = [
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

class ListRepo(BaseRepo):

    cls_result = ListResult
    store: List

    def __init__(self, model, store=None, id_field=None):
        self.model = model
        self.store = [] if store is None else store
        self.id_field = id_field or self.default_id_field

    def delete_by(self, **kwargs):
        old_count = len(self.store)
        self.store = [item for item in self.store if not self._match_kwargs(item, kwargs)]
        new_count = len(self.store)
        return old_count - new_count

    def add(self, item):
        self.store.append(item)

    #def update(self, item):
    #    id = getattr(item, self.id_field)
    #    for repo_item in self.store:
    #        if self._match_kwargs(repo_item, {self.id_field: id}):
    #            for key, val in vars(item):
    #                setattr(repo_item, key, val)
    #            return

    def _match_kwargs(self, item, kwargs):
        return all(
            getattr(item, key) == val
            for key, val in kwargs.items()
        )
