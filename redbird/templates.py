
from abc import abstractmethod
from typing import Iterator
from .base import BaseRepo, BaseResult, Data

class TemplateResult(BaseResult):
    repo: 'TemplateRepo'

    def query_data(self) -> Iterator[Data]:
        "Get actual result"
        return self.repo.query_read(self.query_)

    def update(self, **kwargs):
        return self.repo.query_update(self.query_, kwargs)

    def delete(self):
        return self.repo.query_delete(self.query_)

    def replace(self, __values:dict=None, **kwargs):
        "Replace the existing item(s) with given"
        if __values is not None:
            kwargs.update(__values)
        try:
            return self.repo.query_replace(self.query_, kwargs)
        except NotImplementedError:
            # Has no custom replace implemented, using 
            # default alternative (delete, add)
            return super().replace(**kwargs)

    def count(self):
        try:
            return self.repo.query_count(self.query_)
        except NotImplementedError:
            # Has no custom replace implemented, using 
            # default alternative (len(self))
            return super().count()

    def first(self):
        try:
            return self.repo.query_read_first(self.query_)
        except NotImplementedError:
            return super().first()

    def limit(self, n:int):
        try:
            return self.repo.query_read_limit(self.query_, n)
        except NotImplementedError:
            return super().limit(n)

    def last(self):
        try:
            return self.repo.query_read_last(self.query_)
        except NotImplementedError:
            return super().last()

    def format_query(self, query: dict) -> dict:
        qry = super().format_query(query)
        try:
            return self.repo.format_query(qry)
        except NotImplementedError:
            return qry


class TemplateRepo(BaseRepo):
    cls_result = TemplateResult

    @abstractmethod
    def query_read(self, query):
        ...

    @abstractmethod
    def query_update(self, query, values):
        ...

    @abstractmethod
    def query_delete(self, query):
        ...

    def query_read_first(self, query):
        raise NotImplementedError("Read using first not implemented.")

    def query_read_limit(self, query, n:int):
        raise NotImplementedError("Read using limit not implemented.")

    def query_read_last(self, query):
        raise NotImplementedError("Read using first not implemented.")

    def query_replace(self, query, kwargs):
        raise NotImplementedError("Replacing using query not implemented.")

    def query_count(self, query):
        raise NotImplementedError("Count using query not implemented.")

    def format_query(self, query:dict):
        raise NotImplementedError("Query formatting not implemented.")
