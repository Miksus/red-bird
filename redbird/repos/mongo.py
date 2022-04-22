
from typing import TYPE_CHECKING, Dict, Union


from pydantic import BaseModel, ValidationError

from redbird.base import BaseResult, BaseRepo
from redbird.exc import KeyFoundError
from redbird.oper import GreaterEqual, GreaterThan, LessEqual, LessThan, NotEqual, Operation

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

    def __init__(self, url, binds:Dict[str, str]=None):
        self.url = url
        self.binds = binds if binds is not None else {}

        self._binds = {}
        self._bind = None

    @property
    def client(self):
        if self._bind is None:
            self._bind = self.create_client()
        return self._bind

    def create_client(self, url=None):
        from pymongo import MongoClient
        url = self.url if url is None else url
        return MongoClient(url)

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

class MongoResult(BaseResult):

    repo: 'MongoRepo'
    __operators__ = {
        GreaterThan: "$gt",
        LessThan: "$lt",
        GreaterEqual: "$gte",
        LessEqual: "$lte",
        NotEqual: "$ne",
    }

    def query_data(self):
        col = self.repo.get_collection()
        for data in col.find(self.query_):
            yield data

    def limit(self, n:int):
        "Get n first items"
        col = self.repo.get_collection()
        return [
            self.repo.data_to_item(item)
            for item in col.find(self.query_).limit(n)
        ]

    def update(self, __values:dict=None, **kwargs):
        "Update the resulted rows"
        if __values is not None:
            kwargs.update(__values)
        # TODO: If self.repo.id_field in kwargs?
        col = self.repo.get_collection()
        col.update_many(self.query_, {"$set": kwargs})

    def delete(self):
        "Delete found documents"
        col = self.repo.get_collection()
        col.delete_many(self.query_)

    def count(self):
        "Count found documents"
        col = self.repo.get_collection()
        return col.count_documents(self.query_)

    def format_query(self, query):
        query = super().format_query(query)
        return {
            key: self._get_query_value(val)
            for key, val in query.items()
        }
    
    def _get_query_value(self, value):
        if isinstance(value, Operation):
            return {self.__operators__[type(value)]: value.value}
        else:
            return value

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

    def __init__(self, *args, url=None, database:str=None, collection=None, session=None, **kwargs):
        super().__init__(*args, **kwargs)
        self.session = self.cls_session(url=url) if session is None else session
        self.database = database
        self.collection = collection

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

    def filter_by(self, *args, **kwargs) -> BaseResult:
        # Rename id_field --> "_id"
        if self.id_field in kwargs:
            kwargs["_id"] = kwargs.pop(self.id_field)
        return super().filter_by(*args, **kwargs)

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
        json = self.item_to_dict(item)
        # Rename whatever is as id_field to _id
        json["_id"] = json.pop(self.id_field)
        return json