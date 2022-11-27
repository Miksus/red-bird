
from typing import TYPE_CHECKING, Any, Optional, Type
from pydantic import BaseModel, Field, PrivateAttr
from redbird import BaseRepo, BaseResult
from redbird.templates import TemplateRepo
from redbird.exc import KeyFoundError


from redbird.oper import Between, In, Operation, skip
from redbird.utils.deprecate import deprecated

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
    session : sqlalchemy.session.Session
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
        from sqlalchemy import create_engine
        return cls.from_engine(*args, engine=create_engine(conn_string), **kwargs)

    def __init__(self, *args, reflect_model=False, conn_string=None, engine=None, session=None, if_missing="raise", **kwargs):
        from sqlalchemy import create_engine, inspect
        from sqlalchemy.ext.automap import automap_base
        from sqlalchemy.exc import NoSuchTableError

        # Determine connection
        if conn_string is not None:
            engine = create_engine(conn_string)
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
            table_exists = inspect(engine).has_table(table)
            if not table_exists:
                if if_missing == "raise":
                    raise NoSuchTableError(f"Table {table} is missing. Create the table or pass if_missing='create'")
                elif if_missing == "create":
                    self._create_table(session, kwargs['model'], name=table, primary_column=kwargs.get('id_field'))

            self._Base = automap_base()
            self._Base.prepare(engine=engine, reflect=True)
            try:
                model_orm = self._Base.classes[table]
            except KeyError as exc:
                raise KeyError(f"Cannot automap table '{table}'. Perhaps table missing primary key?") from exc
            kwargs["model_orm"] = model_orm
        if reflect_model:
            kwargs["model"] = self.orm_model_to_pydantic(kwargs["model_orm"])
        super().__init__(*args, session=session, **kwargs)

    def insert(self, item):
        from sqlalchemy.exc import IntegrityError
        row = self.item_to_data(item)
        self.session.add(row)
        if self.autocommit:
            try:
                self.session.commit()
            except IntegrityError as exc:
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
        return self.model_orm(**self.item_to_dict(item))

    def orm_model_to_pydantic(self, model):
        # Turn SQLAlchemy BaseModel to Pydantic BaseModel
        from pydantic_sqlalchemy import sqlalchemy_to_pydantic
        return sqlalchemy_to_pydantic(model)

    @staticmethod
    def create_scoped_session(engine):
        from sqlalchemy.orm import sessionmaker, scoped_session
        Session = sessionmaker(bind=engine)
        return scoped_session(Session)

    def item_to_dict(self, item):
        if isinstance(item, dict):
            return item
        elif hasattr(item, "dict"):
            # Is pydantic
            return item.dict(exclude_unset=True)
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
        from sqlalchemy import Column, orm, true
        stmt = true()
        for column_name, oper_or_value in oper.items():
            column = getattr(self.model_orm, column_name) if self.model_orm is not None else Column(column_name)
            if isinstance(oper_or_value, Operation):
                oper = oper_or_value
                if isinstance(oper, Between):
                    sql_oper = column.between(oper.start, oper.end)
                elif isinstance(oper, In):
                    sql_oper = column.in_(oper.value)
                elif oper is skip:
                    continue
                elif hasattr(oper, "__py_magic__"):
                    magic = oper.__py_magic__
                    oper_method = getattr(column, magic)

                    # Here we form the SQLAlchemy operation, ie.: column("mycol") >= 5
                    sql_oper = oper_method(oper.value)
                else:
                    raise NotImplementedError(f"Not implemented operator: {oper}")
            else:
                value = oper_or_value
                sql_oper = column == value
            stmt &= sql_oper
        return stmt

    def _create_table(self, session, model, name, primary_column=None):
        from sqlalchemy import Table, Column, MetaData
        from sqlalchemy import String, Integer, Float, Boolean
        types = {
            str: String,
            int: Integer,
            float: Float,
            bool: Boolean
        }
        columns = [
            Column(name, types[field.type_], primary_key=name == primary_column)
            for name, field in model.__fields__.items()
        ]
        meta = MetaData()
        table = Table(name, meta, *columns)

        engine = session.get_bind()
        table.create(engine)