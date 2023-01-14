import datetime
from typing import TYPE_CHECKING, Any, Optional, Type
import typing
import sys

from pydantic import BaseModel, Field, PrivateAttr
from redbird import BaseRepo, BaseResult
from redbird.dummy import DummySession
from redbird.templates import TemplateRepo
from redbird.exc import KeyFoundError
from redbird.sql.expressions import Table, to_expression

from redbird.oper import Between, In, Operation, skip
from redbird.utils.deprecate import deprecated

try:
    from typing import Literal
except ImportError: # pragma: no cover
    from typing_extensions import Literal

from redbird.packages import sqlalchemy, pydantic_sqlalchemy

if TYPE_CHECKING:
    from sqlalchemy.engine import Engine

class SQLRepo(TemplateRepo):
    """SQL Repository

    Parameters
    ----------
    conn_string : str, optional
        Connection string to the database. 
        Pass either conn_string, engine or session if
        model_orm is not defined.
    engine : sqlalchemy.engine.Engine, optional
        SQLAlchemy engine to connect the database. 
        Pass either conn_string, engine or session if
        model_orm is not defined.
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
    model_orm : Type of Base, optional
        Subclass of SQL Alchemy representation of the item.
        This is the class that is operated behind the scenes.
    table : str, optional
        Table name where the items lies. Should only be given
        if no model_orm specified.
    session : sqlalchemy.orm.Session
        Connection session to the database.
        Pass either conn_string, engine or session if
        model_orm is not defined.

    Examples
    --------

    .. code-block:: python

        from sqlalchemy import create_engine
        from redbird.repos import SQLRepo

        engine = create_engine('sqlite://')
        repo = SQLRepo(engine=engine, table="my_table")

    You may also supply a session:

    .. code-block:: python

        from sqlalchemy import create_engine
        from sqlalchemy.orm import Session
        from redbird.repos import SQLRepo

        engine = create_engine('sqlite://')
        session = Session(engine)
        repo = SQLRepo(session=session, table="my_table")

    Using ORM model:

    .. code-block:: python

        from sqlalchemy.orm import declarative_base
        
        Base = declarative_base()

        class Car(Base):
            __tablename__ = 'my_table'
            color = Column(String, primary_key=True)
            car_type = Column(String)
            milage = Column(Integer)

        from sqlalchemy import create_engine
        from redbird.repos import SQLRepo

        engine = create_engine('sqlite://')
        repo = SQLRepo(model_orm=Car, engine=engine)

    Using ORM model and reflect Pydantic Model:

    .. code-block:: python

        from sqlalchemy.orm import declarative_base
        
        Base = declarative_base()

        class Car(Base):
            __tablename__ = 'my_table'
            color = Column(String, primary_key=True)
            car_type = Column(String)
            milage = Column(Integer)

        from sqlalchemy import create_engine
        from redbird.repos import SQLRepo

        engine = create_engine('sqlite://')
        repo = SQLRepo(model_orm=Car, reflect_model=True, engine=engine)

    Using ORM model and Pydantic Model:

    .. code-block:: python

        from pydantic import BaseModel
        from sqlalchemy.orm import declarative_base
        
        Base = declarative_base()

        class CarORM(Base):
            __tablename__ = 'my_table'
            color = Column(String, primary_key=True)
            car_type = Column(String)
            milage = Column(Integer)

        class Car(BaseModel):
            id: str
            name: str
            age: int

        from sqlalchemy import create_engine
        from redbird.repos import SQLRepo

        engine = create_engine('sqlite://')
        repo = SQLRepo(model=Car, model_orm=CarORM, engine=engine)
    """

    model_orm: Optional[Any]
    table: Optional[str]
    session: Any
    engine: Optional[Any]
    autocommit: bool = Field(default=True, description="Whether to automatically commit the writes (create, delete, update)")

    ordered: bool = Field(default=True, const=True)
    _Base = PrivateAttr()

    @classmethod
    @deprecated("Please use normal init instead.")
    def from_engine(cls, *args, engine, **kwargs):
        kwargs["session"] = cls.create_scoped_session(engine)
        return cls(*args, **kwargs)

    @classmethod
    @deprecated("Please use normal init instead.")
    def from_connection_string(cls, *args, conn_string, **kwargs):
        return cls.from_engine(*args, engine=sqlalchemy.create_engine(conn_string), **kwargs)

    def __init__(self, *args, reflect_model=False, conn_string=None, engine=None, session=None, if_missing="raise", **kwargs):

        # Determine connection
        if conn_string is not None:
            engine = sqlalchemy.create_engine(conn_string)
        if engine is not None:
            session = self.create_scoped_session(engine)

        # Create model_orm/model
        if "model_orm" not in kwargs:
            if session is None:
                raise TypeError(
                    "Connection cannot be determined. "
                    "Consider using method 'from_connection_string' "
                    "and pass connection string as conn_string"
                )
            table = kwargs["table"]
            engine = session.get_bind()
            table_exists = sqlalchemy.inspect(engine).has_table(table)
            if not table_exists:
                if if_missing == "raise":
                    raise sqlalchemy.exc.NoSuchTableError(f"Table {table} is missing. Create the table or pass if_missing='create'")
                elif if_missing == "create":
                    model = kwargs['model']
                    self._create_table(session, model, name=table, primary_column=kwargs.get('id_field', getattr(model, "__id_field__", None)))

            from sqlalchemy.ext.automap import automap_base
            self._Base = automap_base()
            self._Base.prepare(autoload_with=engine)
            try:
                model_orm = self._Base.classes[table]
            except KeyError as exc:
                raise KeyError(f"Cannot automap table '{table}'. Perhaps table missing primary key?") from exc
            kwargs["model_orm"] = model_orm
        if reflect_model:
            kwargs["model"] = self.orm_model_to_pydantic(kwargs["model_orm"])
        super().__init__(*args, session=session, **kwargs)

    def insert(self, item):
        row = self.item_to_data(item)
        self.session.add(row)
        if self.autocommit:
            try:
                self.session.commit()
            except sqlalchemy.exc.IntegrityError as exc:
                self.session.rollback()
                raise KeyFoundError(f"Item {self.get_field_value(item, self.id_field)} is already in the table.") from exc

    def upsert(self, item):
        row = self.item_to_data(item)

        self.session.merge(row)
        self.session.commit()

    def data_to_item(self, item_orm):
        # Turn ORM item to Pydantic item
        if hasattr(self.model, "from_orm") and self.model.Config.orm_mode:
            # Use Pydantic methods
             return self.model.from_orm(item_orm)
        else:
            # Turn ORM to model using mapping
            d = vars(item_orm)
            d.pop("_sa_instance_state", None)
            return self.model(**d)

    def item_to_data(self, item:BaseModel):
        # Turn Pydantic item to ORM item
        return self.model_orm(**self.item_to_dict(item, exclude_unset=False))

    def orm_model_to_pydantic(self, model):
        # Turn SQLAlchemy BaseModel to Pydantic BaseModel
        return pydantic_sqlalchemy.sqlalchemy_to_pydantic(model)

    @staticmethod
    def create_scoped_session(engine):
        Session = sqlalchemy.orm.sessionmaker(bind=engine)
        return sqlalchemy.orm.scoped_session(Session)

    def item_to_dict(self, item, exclude_unset=True):
        if isinstance(item, dict):
            return item
        elif hasattr(item, "dict"):
            # Is pydantic
            return item.dict(exclude_unset=exclude_unset)
        else:
            d = vars(item)
            d.pop("_sa_instance_state", None)
            return d

    def create(self):
        "Create the database and table"
        self.model_orm.__table__.create(bind=self.session.bind)

    def query_data(self, query):
        for data in self._filter_orm(query):
            yield data

    def query_data_first(self, query):
        item = self._filter_orm(query).first()
        if item is not None:
            return self.data_to_item(item)

    def query_update(self, query, values):
        self._filter_orm(query).update(values)
        if self.autocommit:
            self.session.commit()

    def query_delete(self, query):
        self._filter_orm(query).delete()
        if self.autocommit:
            self.session.commit()

    def query_count(self, query):
        return self._filter_orm(query).count()

    def _filter_orm(self, query):
        session = self.session
        return session.query(self.model_orm).filter(query)

    def format_query(self, oper: dict):
        return to_expression(oper, table=self.model_orm)

    def _create_table(self, session, model, name, primary_column=None):
        table = Table(bind=session.get_bind(), name=name)
        table.create_from_model(model, primary_column=primary_column)

class SQLExprRepo(TemplateRepo):

    table: Optional[str]
    engine: Optional[Any]

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def insert(self, item):
        row = self.item_to_dict(item, exclude_unset=False)
        try:
            self.object.insert(row)
        except sqlalchemy.exc.IntegrityError as exc:
            raise KeyFoundError(f"Inserting item {self.get_field_value(item, self.id_field)} failed.") from exc

    def query_data(self, query):
        for data in self.object.select(query):
            yield data

    def query_data_first(self, query):
        item = self.object.select(query).first()
        if item is not None:
            return self.data_to_item(item)

    def query_update(self, query, values):
        self.object.update(query, values)

    def query_delete(self, query):
        self.object.delete(query)

    def query_count(self, query):
        return self.object.count(query)

    @property
    def object(self) -> Table:
        return Table(bind=self.engine, name=self.table)

    def create(self):
        tbl = self.object
        tbl.create_from_model(self.model, primary_column=self.id_field)

    @property
    def session(self):
        return DummySession()