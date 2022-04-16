
from typing import TYPE_CHECKING, Type
from pydantic import BaseModel
from sqlalchemy import Table, MetaData, Column
from sqlalchemy.orm import mapper
from sqlalchemy.ext.automap import automap_base
from redbird import BaseRepo, BaseResult
from redbird.exc import KeyFoundError

from redbird.oper import Operation

if TYPE_CHECKING:
    from sqlalchemy.engine import Engine

class TableRecord:
    """Represent simple database model"""
    def __init__(self, **kwargs):
        for key, val in kwargs.items():
            setattr(self, key, val)

class SQLAlchemyResult(BaseResult):

    repo: 'SQLRepo'

    def query_data(self):
        for data in self._filter_orm():
            yield data

    def first(self):
        item = self._filter_orm().first()
        if item is not None:
            return self.repo.data_to_item(item)

    def update(self, **kwargs):
        "Update the resulted rows"
        model = self.repo.model
        session = self.repo.session

        self._filter_orm().update(kwargs)

    def delete(self):
        self._filter_orm().delete()

    def count(self):
        return self._filter_orm().count()

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

    def format_greater_than(self, oper:Operation):
        model = self.repo.model
        return 

    def _filter_orm(self):
        session = self.repo.session
        return session.query(self.repo.model_orm).filter(self.query_)


class SQLRepo(BaseRepo):
    """SQL Repository

    Parameters
    ----------
    model : Type of BaseModel, optional
        Pydantic model class to be used as an item model.
        If not provided, model_orm is converted to
        be as such.
    model_orm : Type of Base, optional
        Subclass of SQL Alchemy representation of the item.
        This is the class that is operated behind the scenes.
    table : str, optional
        Table name where the items lies. Should only be given
        if no model_orm specified.
    engine : sqlalchemy.engine.Engine
        SQLAlchemy engine.
    session : sqlalchemy.session.Session

    """
    cls_result = SQLAlchemyResult

    
    model: Type[BaseModel]

    def __init__(self, model:Type[BaseModel]=None, model_orm=None, *, table:str=None, engine:'Engine'=None, session=None, **kwargs):
        self.session = self.create_scoped_session(engine) if session is None else session
        self._table = table
        self.model_orm = model_orm

        model = self.orm_model_to_pydantic(self.model_orm) if model is None else model
        super().__init__(model, **kwargs)    

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

    def create_scoped_session(self, engine):
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
    def model_orm(self):
        if self._model_orm is not None:
            return self._model_orm
        self._Base = automap_base()
        self._Base.prepare(self.session.get_bind(), reflect=True)
        self._model_orm = getattr(self._Base.classes, self._table)
        return self._model_orm

    @model_orm.setter
    def model_orm(self, value):
        self._model_orm = value
