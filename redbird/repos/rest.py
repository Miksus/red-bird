
import urllib.parse as urlparse
from typing import Any, Callable, Dict, Iterable, List, Optional, Union

from pydantic import BaseModel, Field, PrivateAttr

from redbird.packages import requests
from redbird.oper import GreaterEqual, GreaterThan, LessEqual, LessThan, NotEqual, Operation
from redbird.base import BaseResult, BaseRepo
from redbird.templates import TemplateRepo
from redbird.utils.importing import get_import_error

try:
    import requests
except ImportError: # pragma: no cover
    # RESTRepo cannot be used
    class Session:
        def __init__(self, *args, **kwargs):
            # ImportError is raised here
            raise get_import_error("requests")
else:
    class Session(requests.Session):
        """Subclassed requests.Session for more
        unified methods.
        """

        def remove(self):
            self.close()

class RESTRepo(TemplateRepo):

    """REST API Repository

    Parameters
    ----------
    url : str
        Base URL for the API. Should not contain
        query parameters or the item ID. The full
        URL will be "{url}/{id}" if a single item
        is searched, "{url}?{param}={value}" if 
        the endpoint is queried.
    model : Type
        Class of an item in the repository.
        Commonly dict or subclass of Pydantic
        BaseModel. By default dict
    id_field : str, optional
        Attribute or key that identifies each item
        in the repository.
    field_access : {'attr', 'key'}, optional
        How to access a field in an item. Either
        by attribute ('attr') or key ('item').
        By default guessed from the model.
    query : Type, optional
        Query model of the repository.
    errors_query : {'raise', 'warn', 'discard'}
        Whether to raise an exception, warn or discard
        the item in case of validation error in 
        converting data to the item model from
        the repository. By default raise
    headers : dict
        HTTP Headers (for example authentication)
    url_params : dict
        Additional query parameters passed to every 
        request.
    result : str, callable
        Where the list of items is found from the output.

    Examples
    --------

    .. code-block:: python

        repo = MemoryRepo(url="http://example.com/api")

    For services requiring authentication via headers:
    .. code-block:: python

        token = "1234567890"
        repo = RESTRepo(url="http://example.com/api", headers={"Authorization": f"Bearer {token}"})

    You may also supply some URL parameters for every query:

    .. code-block:: python

        repo = RESTRepo(url="http://example.com/api", url_params={"fields": "car_type,car_model,registration_number"})
    """
    result: Optional[Union[str, Callable]]
    url: str
    url_params: dict = {}
    headers: dict = {}

    _session = PrivateAttr()

    ordered: bool = Field(default=False, const=True)

    def insert(self, item):
        json = self.item_to_dict(item, exclude_unset=False)
        self._request(
            "POST",
            self.url,
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

    def query_data(self, query):
        url = query
        page = self._request("GET", url)
        output = page.json()

        result_loc = self.result
        if result_loc is None:
            items = output
        elif isinstance(result_loc, str):
            items = output[result_loc]
        elif isinstance(result_loc, callable):
            items = result_loc(output)
        else:
            raise TypeError(f"Could not locate results from {result_loc}")

        if isinstance(items, (list, tuple, set)):
            for data in items:
                yield data
        else:
            yield items

    def query_delete(self, query):
        url = query
        self._request("DELETE", url)

    def query_update(self, query, values):
        url = query
        self._request("PATCH", url, json=values)

    def query_replace(self, query, values):
        url = query
        self._request("PUT", url, json=values)

    def format_query(self, query:dict) -> str:
        "Turn the query to a form that's understandable by the underlying API"
        id = query.pop(self.id_field) if query is not None and self.id_field in query else None

        url_base = self.url
        url_params = self._get_url_params(query)

        # URL should look like "www.example.com/api/items/{id}?{param}={value}"
        # or "www.example.com/api/items/{id}"
        # or "www.example.com/api/items?{param}={value}"
        # or "www.example.com/api/items"
        if id and url_params:
            return f"{url_base}/{id}?{url_params}"
        elif id:
            return f"{url_base}/{id}"
        elif url_params:
            return f"{url_base}?{url_params}"
        else:
            return url_base

    def _get_url_params(self, query:dict) -> str:
        query = {} if query is None else query
        url_params = self.url_params.copy()
        url_params.update(query)
        return urlparse.urlencode(url_params)

    @property
    def session(self):
        if not hasattr(self, "_session"):
            self._session = Session()
        return self._session
        