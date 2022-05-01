
from typing import TYPE_CHECKING, Any, Optional, Type
from pydantic import BaseModel, PrivateAttr
from redbird import BaseRepo, BaseResult
from redbird.templates import TemplateRepo
from redbird.exc import KeyFoundError

from redbird.oper import Operation

if TYPE_CHECKING:
    from sqlalchemy.engine import Engine



class SQLRepo(TemplateRepo):
    """SQL Repository

    Parameters
    ----------
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
    engine : sqlalchemy.engine.Engine
        SQLAlchemy engine.
    session : sqlalchemy.session.Session

    Examples
    --------

    .. code-block:: python

        from sqlalchemy import create_engine
        from redbird.repos import SQLRepo

        engine = create_engine('sqlite://')
        repo = SQLRepo.from_engine(engine=engine, table="my_table")

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
        repo = SQLRepo.from_engine(model_orm=Car, engine=engine)

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
        repo = SQLRepo.from_engine(model_orm=Car, reflect_model=True, engine=engine)

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
        repo = SQLRepo.from_engine(model=Car, model_orm=CarORM, engine=engine)
    """

    model_orm: Optional[Any]
    table: Optional[str]
    session: Any
    engine: Optional[Any]

    _Base = PrivateAttr()

    @classmethod
    def from_engine(cls, *args, engine, **kwargs):
        kwargs["session"] = cls.create_scoped_session(engine)
        return cls(*args, **kwargs)

    @classmethod
    def from_connection_string(cls, *args, conn_string, **kwargs):
        from sqlalchemy import create_engine
        return cls.from_engine(*args, engine=create_engine(conn_string), **kwargs)


    def __init__(self, *args, reflect_model=False, **kwargs):
        from sqlalchemy.ext.automap import automap_base
        if "model_orm" not in kwargs:
            session = kwargs["session"]
            table = kwargs["table"]
            self._Base = automap_base()
            self._Base.prepare(session.get_bind(), reflect=True)
            kwargs["model_orm"] = getattr(self._Base.classes, table)
        if reflect_model:
            kwargs["model"] = self.orm_model_to_pydantic(kwargs["model_orm"])
        super().__init__(*args, **kwargs)

    # def __init__(self, model:Type[BaseModel]=None, model_orm=None, *, table:str=None, engine:'Engine'=None, session=None, **kwargs):
    #     super().__init__(*args, **kwargs)
    #     self.session = self.create_scoped_session(engine) if session is None else session
    #     self._table = table
    #     self.model_orm = model_orm
    # 
    #     model = self.orm_model_to_pydantic(self.model_orm) if model is None else model
    #     super().__init__(model, **kwargs)    

    def insert(self, item):
        from sqlalchemy.exc import IntegrityError
        row = self.item_to_data(item)

        try:
            self.session.add(row)
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
        self.model_orm.__table__.create(bind=self.session.bind)

    @property
    def model_ormz(self):
        if self._model_orm is not None:
            return self._model_orm
        self._Base = automap_base()
        self._Base.prepare(self.session.get_bind(), reflect=True)
        self._model_orm = getattr(self._Base.classes, self._table)
        return self._model_orm

    @model_ormz.setter
    def model_ormz(self, value):
        self._model_orm = value

    def query_read(self, query):
        for data in self._filter_orm(query):
            yield data

    def query_read_first(self, query):
        item = self._filter_orm(query).first()
        if item is not None:
            return self.data_to_item(item)

    def query_update(self, query, values):

        self._filter_orm(query).update(values)

    def query_delete(self, query):
        self._filter_orm(query).delete()

    def query_count(self, query):
        return self._filter_orm(query).count()

    def _filter_orm(self, query):
        session = self.session
        return session.query(self.model_orm).filter(query)

    def format_query(self, oper: dict):
        from sqlalchemy import column, orm, true
        stmt = true()
        for column_name, oper_or_value in oper.items():
            if isinstance(oper_or_value, Operation):
                oper = oper_or_value
                magic = oper.__py_magic__
                oper_method = getattr(column(column_name), magic)

                # Here we form the SQLAlchemy operation, ie.: column("mycol") >= 5
                sql_oper = oper_method(oper.value)
            else:
                value = oper_or_value
                sql_oper = column(column_name) == value
            stmt &= sql_oper
        return stmt
