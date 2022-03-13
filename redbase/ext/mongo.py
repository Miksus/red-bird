

from abc import abstractmethod, ABC
from ctypes import Union
from typing import Any, Generator, List
from dataclasses import dataclass


from pydantic import BaseModel, ValidationError

from pymongo import MongoClient, client_session
from pymongo.collection import Collection
from pymongo.database import Database
from pymongo.cursor import Cursor
from pymongo.errors import DuplicateKeyError

from redbase.base import BaseResult, BaseRepo
from redbase.exc import KeyFoundError
from redbase.oper import Operation

class MongoSession:

    def __init__(self, url):
        self.url = url
        self._client = None

    @property
    def client(self):
        if self._client is None:
            self._client = self.create_client()
        return self._client

    def create_client(self):
        return MongoClient(self.url)

    def close(self):
        if self._client is not None:
            self._client.close()

    def remove(self):
        """Remove the connection

        Similar in some ways to SQLAlchemy's Session.remove.
        This method will close the client and remove it.
        Next time the client is accessed, a new client is 
        created. Meant to be safe for multi-threaded apps.
        """
        if self._client is not None:
            self._client.close()
            self._client = None

class MongoResult(BaseResult):

    repo: 'MongoRepo'

    def query(self):
        col = self.repo._get_collection()
        for item in col.find(self.query_):
            yield self.repo.parse_item(item)

    def limit(self, n:int):
        "Get n first items"
        col = self.repo._get_collection()
        return [
            self.repo.parse_item(item)
            for item in col.find(self.query_).limit(n)
        ]

    def update(self, **kwargs):
        "Update the resulted rows"
        col = self.repo._get_collection()
        col.update_many(self.query_, {"$set": kwargs})

    def delete(self):
        "Delete found documents"
        col = self.repo._get_collection()
        col.delete_many(self.query_)

    def count(self):
        "Count found documents"
        col = self.repo._get_collection()
        return col.count_documents(self.query_)

    def format_greater_than(self, oper:Operation):
        return {"$gt": oper.value}

    def format_less_than(self, oper:Operation):
        return {"$lt": oper.value}

    def format_greater_equal(self, oper:Operation):
        return {"$gte": oper.value}

    def format_less_equal(self, oper:Operation):
        return {"$lte": oper.value}

    def format_not_equal(self, oper:Operation):
        return {"$ne": oper.value}

class MongoRepo(BaseRepo):
    """MongoDB Repository

    Parameters
    ----------
    model : BaseModel
        Class of a document
    url : str
        Connection string to the database
    session : Session, Any
        A MongoDB session object that should
        have at least ``client`` attribute
    id_field : str
        Field/attribute that identifies a 
        document from others. 
    """
    model: BaseModel = None
    cls_result = MongoResult
    default_id_field = "_id"
    cls_session = MongoSession

    def __init__(self, model:BaseModel, url=None, session=None, id_field=None):
        self.model = model
        self.session = self.cls_session(url=url) if session is None else session
        self.id_field = id_field or self.default_id_field

    def insert(self, item):
        col = self._get_collection()
        doc = self.format_item(item)
        try:
            col.insert_one(doc)
        except DuplicateKeyError as exc:
            raise KeyFoundError(f"Document {getattr(item, self.id_field)} already exists.") from exc

    def upsert(self, item):
        col = self._get_collection()
        doc = self.format_item(item)
        col.update_one({"_id": getattr(item, self.id_field)}, {"$set": doc}, upsert=True)

    #def update(self, item):
    #    col = self._get_collection()
    #    d = self.parse_item(item)
    #    col.update_one(
    #        {self.id_field: d[self.id_field]},
    #        d
    #    )

    def delete_by(self, **kwargs):
        col = self._get_collection()
        return col.delete_many(kwargs).deleted_count
    
    def filter_by(self, **kwargs) -> BaseResult:
        # Rename id_field --> "_id"
        if self.id_field in kwargs:
            kwargs["_id"] = kwargs.pop(self.id_field)
        return super().filter_by(**kwargs)

    def _get_collection(self) -> 'Collection':
        database = self._get_database()
        return database[self.model.__colname__]

    def _get_database(self) -> Database:
        client = self._get_client()
        return client.get_default_database()

    def _get_client(self) -> MongoClient:
        # bind = self.model.__bind_key__
        return self.session.client

    def _format_dict(self, item:dict) -> BaseModel:
        try:
            return self.model(**item)
        except ValidationError as exc:
            raise ValidationError(f"Formatting for {item[self.id_field]} failed.") from exc

    def parse_item(self, json:dict):
        # Rename _id to whatever is as id_field
        json[self.id_field] = json.pop("_id")
        return self.model(**json)

    def format_item(self, item:BaseModel):
        json = item.dict()
        # Rename whatever is as id_field to _id
        json["_id"] = json.pop(self.id_field)
        return json