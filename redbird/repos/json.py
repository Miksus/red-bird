
from operator import getitem
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional, Union, TextIO

import csv, json

from pydantic import BaseModel, PrivateAttr
from redbird import BaseRepo, BaseResult
from redbird.base import Data, Item
from redbird.templates import TemplateRepo
from redbird.exc import KeyFoundError
from redbird.oper import Operation
from redbird.dummy import DummySession
from redbird.utils.query import delete_items, read_items, update_items

from redbird.utils.query import QueryMatcher

class JSONDirectoryRepo(TemplateRepo):
    """JSON directory repository

    This repository represents a directory which 
    contains JSON files. Each item represents a
    file and the ``id_field`` is the stem of the
    file.

    Parameters
    ----------
    path : Path-like
        Path to the repository directory
    model : Type
        Class of an item in the repository.
        Commonly dict or subclass of Pydantic
        BaseModel. By default dict
    id_field : str
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
    kwds_json_load : dict
        Keyword arguments passed to ``json.load``. 
    kwds_json_dump : dict
        Keyword arguments passed to ``json.dump``.
        Useful for prettifying the files

    
    Examples
    --------
    .. code-block:: python

        repo = JSONDirectoryRepo(path="path/to/repo", id_field="id")

    """

    path: Path
    field_names: Optional[List[str]] = None
    kwds_json_load: dict = {}
    kwds_json_dump: dict = {}
    id_field: str

    _session = PrivateAttr()

    def insert(self, item):
        id = self.get_field_value(item, self.id_field)
        file = self.get_file_path(id)
        if file.exists():
            raise KeyFoundError("Item already exists")
        self.write_file(item)

    def query_items(self, query: dict) -> Item:
        filename_query = query.get(self.id_field, None)
        yield from read_items(self, self.read_data(filename_query), query)

    def query_update(self, query:dict, values:dict):
        filename_query = query.get(self.id_field, None)
        new_items = update_items(self, self.read_data(filename_query), query, values, return_="updated")
        self.write_files(new_items)

    def query_delete(self, query:dict):
        filename_query = query.get(self.id_field, None)
        new_items = delete_items(self, self.read_data(filename_query), query, return_="deleted")
        self.delete_files(new_items)

    def read_data(self, id_value: Union[str, Operation]) -> Iterator[Data]:
        optimized = id_value is not None
        if optimized:
            # We don't need to open all files
            if isinstance(id_value, Operation):
                raise NotImplementedError("Operation ID search not yet implemented")
            else:
                try:
                    yield self.read_file(id_value)
                except FileNotFoundError as exc:
                    raise KeyError('Item not found') from exc
        else:
            # We need to open all files
            for file in self.path.glob("*.json"):
                id = file.stem
                yield self.read_file(id)

    def read_file(self, id) -> Data:
        "Read repository file"
        filename = self.get_file_path(id)
        with open(filename, "r") as file:
            return json.load(file, **self.kwds_json_load)

    def write_files(self, items: Iterator[Item]):
        "Write the files with new content of items"
        for item in items:
            self.write_file(item)

    def write_file(self, item: Item):
        "Write to a file with new content of an item"
        id_value = self.get_field_value(item, self.id_field)
        filename = self.get_file_path(id_value)
        with open(filename, "w") as file:
            data = self.item_to_dict(item, exclude_unset=False)
            json.dump(data, file, **self.kwds_json_dump)

    def delete_files(self, items: Iterator[Item]):
        for item in items:
            id_value = self.get_field_value(item, self.id_field)
            filename = self.get_file_path(id_value)
            filename.unlink()

    def get_writer(self, buff: TextIO):
        return csv.DictWriter(buff, fieldnames=self.get_headers(), **self.kwds_csv)

    def get_reader(self, buff: TextIO):
        return csv.DictReader(buff, fieldnames=self.get_headers(), **self.kwds_csv)

    def create(self, if_exists="raise"):
        "Create the repository file"
        if if_exists == "raise" and self.filename.exists():
            raise FileExistsError(f"Repository file {self.filename} already exists.")
        with open(self.filename, "w", newline="") as file:
            writer = self.get_writer(file)
            writer.writeheader()

    def data_to_item(self, data: dict) -> Item:
        # None values are converted to empty strings
        # Therefore we interpret empty strings as None
        data = {key: None if val == "" else val for key, val in data.items()}
        return super().data_to_item(data)

    def get_file_path(self, id):
        return self.path / f"{id}.json"

    def create(self):
        "Create the repository directory"
        self.path.mkdir(parents=True, exist_ok=True)

    @property
    def session(self):
        if not hasattr(self, "_session"):
            self._session = DummySession()
        return self._session