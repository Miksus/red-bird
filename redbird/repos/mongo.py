
from typing import TYPE_CHECKING, Any, Dict, Optional, Union


from pydantic import BaseModel, Field, ValidationError

from redbird.base import BaseResult, BaseRepo
from redbird.exc import KeyFoundError
from redbird.oper import Between, GreaterEqual, GreaterThan, In, LessEqual, LessThan, NotEqual, Operation, skip
from redbird.templates import TemplateRepo
from redbird.packages import pymongo

if TYPE_CHECKING:
    from pymongo import MongoClient
    from pymongo.database import Database
    from pymongo.collection import Collection    

class MongoSession:

    """MongoDB session

    Similar as SQLAlchemy Session but works slightly 
    different due to technical details

    MongoClient is analogous to sqlalchemy.engine.Engine but 
    MongoClient is not fork safe: https://pymongo.readthedocs.io/en/stable/faq.html#is-pymongo-fork-safe
    SQLAlchemy Engine is fork safe if no connections are made.

    This object is fork safe if no clients are open (call remove to close all of them).
    """

    url: str
    binds: Dict[str, str]

    _bind: 'MongoClient'
    _binds: Dict[str, 'MongoClient']

    def __init__(self, url=None, binds:Dict[str, str]=None, client=None):
        self.url = url
        self.binds = binds if binds is not None else {}

        self._binds = {}
        self._bind = client

    @property
    def client(self):
        if self._bind is None:
            self._bind = self.create_client()
        return self._bind

    def create_client(self, url=None):
        url = self.url if url is None else url
        return pymongo.MongoClient(url)

    def close(self):
        "Close client and all binds"
        if self._bind is not None:
            self._bind.close()
        for client in self._binds.values():
            client.close()

    def remove(self):
        """Remove the connection

        Similar in some ways to SQLAlchemy's Session.remove.
        This method will close the clients and remove them.
        Next time the client is accessed, a new client is 
        created. Meant to be safe for multi-threaded apps.
        """
        self.close()

        self._bind = None
        self._binds = {}

    def get_bind(self, bind_key=None) -> 'MongoClient':
        "Get client associated with the mapper"
        if bind_key is None:
            # return default connection
            return self.client
        else:
            bind_key = getattr(bind_key, "__bind_key__", bind_key)
            url = self.binds.get(bind_key, self.url)
            if url not in self._binds:
                self._binds[url] = self.create_client(url)
            return self._binds[url]

class MongoRepo(TemplateRepo):
    """MongoDB Repository

    Parameters
    ----------
    uri : str, optional
        Connection string to the database.
        Pass uri, client or session.
    client : mongodb.MongoClient, optional
        MongoDB client for the connection.
        Pass uri, client or session.
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
    session : Session, Any
        A MongoDB session object that should
        have at least ``client`` attribute.
        Pass uri, client or session.

    Examples
    --------
    .. code-block:: python

        repo = MongoRepo(uri="mongodb://localhost:27017/mydb?authSource=admin", collection="mycol")

    .. code-block:: python

        repo = MongoRepo(uri="mongodb://localhost:27017", database="mydb", collection="mycol")

    .. code-block:: python

        from pymongo import MongoClient
        repo = MongoRepo(client=MongoClient("mongodb://localhost:27017"))
    """
    # cls_result = MongoResult
    default_id_field = "_id"
    cls_session = MongoSession

    __operators__ = {
        GreaterThan: "$gt",
        LessThan: "$lt",
        GreaterEqual: "$gte",
        LessEqual: "$lte",
        NotEqual: "$ne",
        In: "$in",
    }
    session: Any
    database: Optional[str]
    collection: Optional[str]

    ordered: bool = Field(default=True, const=True)

    def __init__(self, *args, uri=None, client=None, session=None, **kwargs):
        if uri is not None:
            session = MongoSession(url=uri)
        if client is not None:
            session = MongoSession(client=client)
        if session is None:
            raise TypeError(
                "Cannot determine the connection. "
                "Consider passing 'uri', 'client' or 'session'."
            )
        super().__init__(*args, session=session, **kwargs)

    @classmethod
    def from_uri(cls, *args, uri, **kwargs):
        kwargs["session"] = MongoSession(url=uri)
        return cls(*args, **kwargs)

    @classmethod
    def from_client(cls, *args, client, **kwargs):
        kwargs["session"] = MongoSession(client=client)
        return cls(*args, **kwargs)

    def insert(self, item):
        from pymongo.errors import DuplicateKeyError
        col = self.get_collection()
        doc = self.item_to_data(item)
        try:
            col.insert_one(doc)
        except DuplicateKeyError as exc:
            raise KeyFoundError(f"Document {self.get_field_value(item, self.id_field)} already exists.") from exc

    def upsert(self, item):
        col = self.get_collection()
        doc = self.item_to_data(item)
        col.update_one({"_id": self.get_field_value(item, self.id_field)}, {"$set": doc}, upsert=True)

    def filter_by(self, **kwargs) -> BaseResult:
        # Rename id_field --> "_id"
        if self.id_field in kwargs:
            kwargs["_id"] = kwargs.pop(self.id_field)
        return super().filter_by(**kwargs)

    def get_collection(self) -> 'Collection':
        "Get the MongoDB collection object"
        col_name = self.model.__colname__ if hasattr(self.model, "__colname__") else self.collection
        database = self.get_database()
        return database[col_name]

    def get_database(self) -> 'Database':
        "Get the MongoDB database object"
        client = self.get_client()
        return client[self.database] if self.database is not None else client.get_default_database()

    def get_client(self) -> 'MongoClient':
        "Get the MongoDB client"
        #! TODO: Support __bind_key__
        return self.session.get_bind(self.model)

    def _format_dict(self, item:dict) -> BaseModel:
        try:
            return self.model(**item)
        except ValidationError as exc:
            raise ValidationError(f"Formatting for {item[self.id_field]} failed.") from exc

    def data_to_item(self, json:dict):
        # Rename _id to whatever is as id_field
        json[self.id_field] = json.pop("_id")
        return self.model(**json)

    def item_to_data(self, item:BaseModel):
        json = self.item_to_dict(item, exclude_unset=False)
        # Rename whatever is as id_field to _id
        json["_id"] = json.pop(self.id_field)
        return json

# Query based
    def query_data(self, query):
        col = self.get_collection()
        for data in col.find(query):
            yield data

    def query_data_limit(self, query, n:int):
        "Get n first items"
        col = self.get_collection()
        return [
            self.data_to_item(item)
            for item in col.find(query).limit(n)
        ]

    def query_update(self, query, values):
        col = self.get_collection()
        col.update_many(query, {"$set": values})

    def query_delete(self, query):
        col = self.get_collection()
        col.delete_many(query)

    def query_count(self, query):
        col = self.get_collection()
        return col.count_documents(query)

    def format_query(self, query):
        return {
            key: self._get_query_value(val)
            for key, val in query.items()
            if val is not skip
        }

    def _get_query_value(self, value):
        if isinstance(value, Operation):
            type_ = type(value)
            if type_ in self.__operators__:
                return {self.__operators__[type_]: value.value}
            elif type_ == Between:
                return {
                    '$gte': value.start,
                    '$lte': value.end,
                }
            else:
                raise NotImplementedError("MongoRepo does not yet support operator: {type_}")
        else:
            return value
