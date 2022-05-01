

from typing import Any, Dict, Iterable, List, Union

from redbird.oper import GreaterEqual, GreaterThan, LessEqual, LessThan, NotEqual, Operation
from .base import BaseResult, BaseRepo

class DummySession:
    """Dummy session

    Imitates similar session objects as SQLAlchemy's
    session in order to avoid code changes if
    in-memory repository is used.
    """

    def close(self):
        "Close the connection(s)/client(s)/engine(s)"
        ...
    
    def remove(self):
        "Close the connection(s)/client(s)/engine(s) so that they can be recreated"
        ...

    def get_bind(self, bind_key=None):
        "Get connection/client/engine for given key"
        ...

class DummyData:
    """Dummy data model from the repository database
    
    This class does nothing and is for testing
    the API and acting as a look-up template.
    """


class DummyModel:
    """Dummy item model
    
    This class does nothing and is for testing
    the API and acting as a look-up template.
    """

    def __init__(self, **kwargs):
        for key, val in kwargs.items():
            setattr(self, key, val)

class DummyResult(BaseResult):
    """Dummy query results

    This class does nothing and is for testing
    the API and acting as a look-up template.
    """

    query_: Dict[str, Any]
    repo: 'DummyRepo'

    def query(self) -> Iterable[DummyModel]:
        "Iterate over the records according to self.query_"
        ...
        yield

    def update(self, **kwargs):
        "Update records according to self.query_"
        ...

    def delete(self):
        "Delete records according to self.query_"
        ...

    def first(self) -> DummyModel:
        "Get first record according to self.query_"
        # Optional
        ...

    def last(self) -> DummyModel:
        "Get last record according to self.query_"
        # Optional
        ...

    def limit(self, n:int) -> List[DummyModel]:
        "Get n records according to self.query_"
        # Optional (recommended)
        ...

    def count(self) -> int:
        "Count the number of records according to self.query_"
        # Optional (recommended)
        ...
    
    def format_query(self, query:Dict[str, Union[Any, Operation]]) -> Dict[str, Any]:
        "Format the query so that the database understands it"
        # Optional (recommended)
        ...

    def format_greater_than(self, operation:GreaterThan) -> Any:
        "Format greater than operation so that the database understands it"
        # Optional, called by format_query
        ...

    def format_less_than(self, operation:LessThan) -> Any:
        "Format less than operation so that the database understands it"
        # Optional, called by format_query
        ...

    def format_less_equal(self, operation:LessEqual) -> Any:
        "Format less equal operation so that the database understands it"
        # Optional, called by format_query
        ...

    def format_greater_equal(self, operation:GreaterEqual) -> Any:
        "Format greater equal operation so that the database understands it"
        # Optional, called by format_query
        ...

    def format_not_equal(self, operation:NotEqual) -> Any:
        "Format not equal operation so that the database understands it"
        # Optional, called by format_query
        ...

class DummyRepo(BaseRepo):
    """Dummy repository

    This class does nothing and is for testing
    the API and acting as a look-up template.
    """

    #model: DummyModel
    #cls_result: DummyResult

    def __init__(self, model: DummyModel, **kwargs):
        self.model = model

    def insert(self, item: DummyModel):
        "Insert one item to the repository"
        ...

    def upsert(self, item: DummyModel):
        "Insert one item to the repository and update if exists"
        # Optional
        ...

    def filter_by(self, **kwargs) -> DummyResult:
        "Filter the repository according to key-value arguments"
        # Optional
        ...

    def data_to_item(self, data: DummyData) -> DummyModel:
        "Transform a database object to item object"
        # Optional
        ...

    def item_to_data(self, item: DummyModel) -> DummyData:
        "Transform an item object to a database object"
        # Optional
        ...