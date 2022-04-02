
import urllib.parse as urlparse
from typing import Any, Dict, Iterable, List, Union

from pydantic import BaseModel

from redbase.oper import GreaterEqual, GreaterThan, LessEqual, LessThan, NotEqual, Operation
from redbase.base import BaseResult, BaseRepo

import requests

class RESTResult(BaseResult):

    def query(self):
        url = self.repo.get_url(self.query_)
        output = self.repo._request("GET", url).json()
        if isinstance(output, (list, tuple, set)):
            for item in output:
                yield self.repo.parse_item(item)
        else:
            yield self.repo.parse_item(output)

    def delete(self):
        url = self.repo.get_url(self.query_)
        self.repo._request("DELETE", url)

    def update(self, **kwargs):
        url = self.repo.get_url(self.query_)
        self.repo._request("PATCH", url, json=kwargs)

    def replace(self, **kwargs):
        url = self.repo.get_url(self.query_)
        self.repo._request("PUT", url, json=kwargs)


class RESTRepo(BaseRepo):

    cls_result = RESTResult

    def __init__(self, model:BaseModel, url, id_field=None):
        self.model = model
        self.url = url
        self.id_field = id_field
        self.session = requests.Session()

    def insert(self, item):
        json = self.item_to_dict(item)
        self._request(
            "POST",
            self.get_url(),
            json=json
        )

    def replace(self, item):
        qry = {self.id_field: getattr(item, self.id_field)}
        values = self.item_to_dict(item)
        self.filter_by(**qry).replace(**values)

    def _request(self, *args, **kwargs):
        return self.session.request(
            *args, **kwargs
        )

    def get_url(self, query=None):
        query = {} if query is None else query
        id = query.pop(self.id_field) if self.id_field in query else None

        url_base = self.url
        url_params = urlparse.urlencode(query)

        if id is None:
            id = ""
        elif not id.startswith("/"):
            id = "/" + id
        if url_params:
            url_params = "?" + url_params

        return f"{url_base}{id}{url_params}"