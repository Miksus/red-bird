
from typing import List
from redrepo import BaseRepo, BaseResult

class ListResult(BaseResult):

    store: List
    repo: 'ListRepo'

    def query(self):
        l = self.repo.store
        for item in l:
            if self.repo._match_kwargs(item, self.query_):
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
            if not self.repo._match_kwargs(item, self.query_)
        ]

class ListRepo(BaseRepo):

    cls_result = ListResult
    store: List

    def __init__(self, cls_item, store=None, id_field=None):
        self.cls_item = cls_item
        self.store = [] if store is None else store
        self.id_field = id_field or self.default_id_field

    def delete_by(self, **kwargs):
        old_count = len(self.store)
        self.store = [item for item in self.store if not self._match_kwargs(item, kwargs)]
        new_count = len(self.store)
        return old_count - new_count

    def add(self, item):
        self.store.append(item)

    def update(self, item):
        id = getattr(item, self.id_field)
        for repo_item in self.store:
            if self._match_kwargs(repo_item, {self.id_field: id}):
                for key, val in vars(item):
                    setattr(repo_item, key, val)
                return

    def _match_kwargs(self, item, kwargs):
        return all(
            getattr(item, key) == val
            for key, val in kwargs.items()
        )
