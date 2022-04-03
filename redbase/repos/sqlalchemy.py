
from typing import TYPE_CHECKING, Type
from pydantic import BaseModel
from redbase import BaseRepo, BaseResult
from redbase.exc import KeyFoundError

from redbase.oper import Operation

if TYPE_CHECKING:
    from sqlalchemy.engine import Engine

class SQLAlchemyResult(BaseResult):

    repo: 'SQLRepo'

    def query(self):
        for item in self._filter_orm():
            yield self.repo.data_to_item(item)

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
    model_orm : Type of Base
        Subclass of SQL Alchemy representation of the item.
        This is the class that is operated behind the scenes.
    engine : sqlalchemy.engine.Engine
        SQL Alchemy engine.
    session : sqlalchemy.session.Session

    """
    cls_result = SQLAlchemyResult

    
    model: Type[BaseModel]

    def __init__(self, model:Type[BaseModel]=None, model_orm=None, *, engine:'Engine'=None, session=None, **kwargs):
        super().__init__(model, **kwargs)
        self.model_orm = model_orm
        self.model = self.parse_model(model_orm) if model is None else model
        self.session = self.create_scoped_session(engine) if session is None else session

    def insert(self, item):
        from sqlalchemy.exc import IntegrityError
        row = self.item_to_data(item)

        try:
            self.session.add(row)
            self.session.commit()
        except IntegrityError as exc:
            self.session.rollback()
            raise KeyFoundError(f"Item {getattr(item, self.id_field)} is already in the table.") from exc

    def upsert(self, item):
        row = self.item_to_data(item)

        self.session.merge(row)
        self.session.commit()

    def data_to_item(self, item_orm):
        # Turn ORM item to Pydantic item
        return self.model.from_orm(item_orm)

    def item_to_data(self, item:BaseModel):
        # Turn Pydantic item to ORM item
        return self.model_orm(**item.dict(exclude_unset=True))

    def parse_model(self, model):
        # Turn SQLAlchemy BaseModel to Pydantic BaseModel
        from pydantic_sqlalchemy import sqlalchemy_to_pydantic
        return sqlalchemy_to_pydantic(model)

    def create_scoped_session(self, engine):
        from sqlalchemy.orm import sessionmaker, scoped_session
        Session = sessionmaker(bind=engine)
        return scoped_session(Session)

    def item_to_dict(self, item):
        if hasattr(item, "dict"):
            # Is pydantic
            return item.dict(exclude_unset=True)
        else:
            d = vars(item)
            d.pop("_sa_instance_state", None)
            return d

    def create(self):
        self.model_orm.__table__.create(bind=self.session.bind)