
import urllib.parse as urlparse
from typing import Any, Dict, Iterable, List, Optional, Union

from pydantic import BaseModel

from redbird.oper import GreaterEqual, GreaterThan, LessEqual, LessThan, NotEqual, Operation
from redbird.base import BaseResult, BaseRepo

import requests

class Session(requests.Session):
    """Subclassed requests.Session for more
    unified methods.
    """

    def remove(self):
        self.close()

class RESTResult(BaseResult):

    repo: 'RESTRepo'
    def query(self):
        url = self.repo.get_url(self.query_)
        page = self.repo._request("GET", url)
        output = page.json()

        result_loc = self.repo.result
        if result_loc is None:
            items = output
        elif isinstance(result_loc, str):
            items = output[result_loc]
        elif isinstance(result_loc, callable):
            items = result_loc(output)
        else:
            raise TypeError(f"Could not locate results from {result_loc}")

        if isinstance(items, (list, tuple, set)):
            for item in items:
                yield self.repo.data_to_item(item)
        else:
            yield self.repo.data_to_item(items)

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

    """REST API Repository

    Parameters
    ----------
    url : str
        Base URL for the API. Should not contain
        query parameters or the item ID. The full
        URL will be "{url}/{id}" if a single item
        is searched, "{url}?{param}={value}" if 
        the endpoint is queried.
    headers : dict
        HTTP Headers (for example authentication)
    url_params : dict
        Additional query parameters passed to every 
        request.
    result : str, callable
        Where the list of items is found from the output.
    """

    cls_result = RESTResult
    result: Optional[Union[str, callable]]

    def __init__(self, *args, url, headers:dict=None, url_params:dict=None, result=None, **kwargs):
        super().__init__(*args, **kwargs)
        self.url = url
        self.session = Session()

        self.headers = headers
        self.url_params = {} if url_params is None else url_params
        self.result = result

    def insert(self, item):
        json = self.item_to_dict(item)
        self._request(
            "POST",
            self.get_url(),
            json=json
        )

    def replace(self, item):
        qry = {self.id_field: self.get_field_value(item, self.id_field)}
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
        id = url_params.pop(self.id_field) if query is not None and self.id_field in query else None

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