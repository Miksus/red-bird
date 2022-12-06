
from functools import wraps
from operator import getitem
from typing import Dict, Generator, List
import warnings

from redbird.base import Data, Item
from redbird.exc import ConversionWarning, _handle_conversion_error
from redbird.templates import TemplateRepo
from redbird.oper import Operation

try:
    from typing import Literal
except ImportError: # pragma: no cover
    from typing_extensions import Literal

class QueryMatcher:

    def __init__(self, query, value_getter):
        self.query = query
        self.value_getter = value_getter

    def __contains__(self, item):
        "Match whether item fulfills the query"
        return all(
            val.evaluate(self.get_value(item, key)) if isinstance(val, Operation) else self.get_value(item, key) == val
            for key, val in self.query.items()
        )

    def get_value(self, item, key):
        try:
            return self.value_getter(item, key)
        except KeyError:
            return None

def read_items(repo: TemplateRepo, reader: Generator, query: dict) -> List[Item]:
    
    query = QueryMatcher(query, value_getter=repo.get_field_value)
    for data in reader:
        try:
            item = repo.data_to_item(data)
        except ValueError:
            _handle_conversion_error(repo, data)
        else:
            if item in query:
                yield item

def update_items(repo: TemplateRepo, reader: Generator, query:dict, values:Dict, return_:Literal['all', 'updated']='all') -> List[Item]:
    query = QueryMatcher(query, value_getter=repo.get_field_value)
    for data in reader:
        item = repo.data_to_item(data)
        is_updated = item in query
        if is_updated:
            for key, val in values.items():
                repo.set_field_value(item, key, val)
        
        if return_ == 'all':
            yield item
        elif return_ == 'updated' and is_updated:
            yield item

def delete_items(repo: TemplateRepo, reader: Generator, query:dict, return_:Literal['remained', 'deleted']='remained') -> List[Item]:
    query = QueryMatcher(query, value_getter=repo.get_field_value)
    for data in reader:
        item = repo.data_to_item(data)
        is_deleted = item in query

        if is_deleted and return_ == 'deleted':
            yield item
        elif not is_deleted and return_ == 'remained':
            yield item
