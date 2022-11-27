
from operator import getitem
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional, Union, TextIO

import csv

from pydantic import BaseModel, Field, PrivateAttr
from redbird import BaseRepo, BaseResult
from redbird.base import Data, Item
from redbird.templates import TemplateRepo
from redbird.exc import KeyFoundError
from redbird.oper import Operation
from redbird.dummy import DummySession
from redbird.utils.query import delete_items, read_items, update_items

from redbird.utils.query import QueryMatcher

class CSVFileRepo(TemplateRepo):
    """CSV file repository

    This repository has a CSV (comma-separated values) 
    file as a data store. Each item represents a row
    in the file.

    Parameters
    ----------
    filename : path-like
        The repository file
    fieldnames : list of str
        Names of the columns in the CSV file.
        If unspecified, model's fields are used
        instead
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
    kwds_csv : dict
        Keyword arguments used to create 
        ``csv.DictWriter`` and ``csv.DictReader``
    
    Examples
    --------

    .. code-block:: python

        repo = CSVFileRepo(filepath="path/to/repo", fieldnames=['id', 'name', 'age'])
    """

    filename: Path
    fieldnames: Optional[List[str]] = None
    kwds_csv: dict = {}

    _session = PrivateAttr()
    ordered: bool = Field(default=True, const=True)

    def insert(self, item):
        file_non_zero = self.filename.exists() and self.filename.stat().st_size > 0
        if not file_non_zero:
            self.create(if_exists="ignore")
        if self.id_field is not None:
            id_ = self.get_field_value(item, self.id_field)
            if id_ in [self.get_field_value(col_item, self.id_field) for col_item in self.filter_by().all()]:
                raise KeyFoundError(f"Item {id_} already in the collection.")
        self.append_file(item)

    def query_items(self, query) -> Item:
        yield from read_items(self, self.read_file(), query)

    def query_update(self, query:dict, values:dict):
        new_items = update_items(self, self.read_file(), query, values, return_='all')
        self.write_file(list(new_items))

    def query_delete(self, query:dict):
        new_items = delete_items(self, self.read_file(), query, return_='remained')
        self.write_file(list(new_items))

    @property
    def session(self):
        if not hasattr(self, "_session"):
            self._session = DummySession()
        return self._session

    def get_headers(self) -> List[str]:
        "Get headers of the CSV file (using the model)"
        if self.fieldnames is not None:
            return self.fieldnames
        elif hasattr(self.model, "__fields__"):
            return list(self.model.__fields__)
        else:
            raise TypeError("Cannot determine CSV headers")

    def read_file(self) -> Iterator[dict]:
        "Read repository file"
        if not self.filename.is_file():
            # The repository does not exists, creating
            self.create(if_exists="ignore")
            return
        with open(self.filename, "r") as file:
            reader = self.get_reader(file)

            # Skip headers
            next(reader, None)

            # Iterate rows
            for data in reader:
                yield data

    def write_file(self, items: List):
        "Write the file with new content of items"
        with open(self.filename, "w", newline="") as file:
            writer = self.get_writer(file)
            writer.writeheader()
            for item in items:
                d = self.item_to_dict(item, exclude_unset=False)
                writer.writerow(d)

    def append_file(self, item: Item):
        "Write a single item at the end of the file"
        with open(self.filename, "a", newline="") as file:
            writer = self.get_writer(file)
            d = self.item_to_dict(item, exclude_unset=False)
            writer.writerow(d)

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
