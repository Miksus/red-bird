
import urllib.parse as urlparse
from typing import Any, Dict, Iterable, List, Union

from pydantic import BaseModel

from redbase.oper import GreaterEqual, GreaterThan, LessEqual, LessThan, NotEqual, Operation
from redbase.base import BaseResult, BaseRepo

import requests

class RESTResult(BaseResult):

    def query(self):
        url = self.repo.get_url(self.query_)
        page = self.repo._request("GET", url)
        output = page.json()
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

    def __init__(self, model:BaseModel, url, id_field=None, headers:dict=None, url_params:dict=None):
        self.model = model
        self.url = url
        self.id_field = id_field
        self.session = requests.Session()

        self.headers = headers
        self.url_params = {} if url_params is None else url_params

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
        page = self.session.request(
            *args, **kwargs, headers=self.headers
        )
        return page

    def get_url(self, query=None):
        url_params = self.url_params.copy()
        if query is not None:
            url_params.update(query)
        id = query.pop(self.id_field) if query is not None and self.id_field in query else None

        url_base = self.url
        url_params = urlparse.urlencode(url_params) # Turn {"param": "value"} --> "param=value"

        if id is None:
            id = ""
        elif not id.startswith("/"):
            id = "/" + id
        if url_params:
            url_params = "?" + url_params

        # URL should look like "www.example.com/api/items/{id}?{param}={value}"
        # or "www.example.com/api/items/{id}"
        # or "www.example.com/api/items?{param}={value}"
        # or "www.example.com/api/items"
        return f"{url_base}{id}{url_params}"