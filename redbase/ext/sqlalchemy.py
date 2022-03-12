
from typing import Type
from pydantic import BaseModel
from redbase import BaseRepo, BaseResult
from sqlalchemy import Column, column, orm, true
from sqlalchemy.sql import True_

from redbase.operation import Operation

class SQLAlchemyResult(BaseResult):

    repo: 'SQLAlchemyRepo'

    def query(self):
        for item in self._filter_orm():
            yield self.repo.parse_item(item)

    def first(self):
        item = self._filter_orm().first()
        if item is not None:
            return self.repo.parse_item(item)

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


class SQLAlchemyRepo(BaseRepo):
    """SQLAlchemy Repository

    Examples
    --------
        .. code-block: python

            repo = Messages(model)
            msg = repo["my_msg_id"]
    """
    cls_result = SQLAlchemyResult

    
    model: BaseModel

    def __init__(self, model:Type[BaseModel]=None, model_orm=None, *, engine, id_field=None):
        self.model_orm = model_orm
        self.model = self.parse_model(model_orm) if model is None else model
        self.session = self.create_scoped_session(engine)
        self.id_field = id_field or self.default_id_field

    def delete_by(self, **kwargs):
        model = self.model
        session = self.session
        return session.query(model).filter_by(**kwargs).delete()

    def add(self, item):
        self.session.add(item)
        self.session.commit()

    def parse_item(self, item_orm):
        # Turn ORM item to Pydantic item
        return self.model.from_orm(item_orm)

    def format_item(self, item:BaseModel):
        # Turn Pydantic item to ORM item
        return self.model_orm(**item.dict())

    def parse_model(self, model):
        # Turn SQLAlchemy BaseModel to Pydantic BaseModel
        from pydantic_sqlalchemy import sqlalchemy_to_pydantic
        return sqlalchemy_to_pydantic(model)

    #def update(self, item):
    #    self.session.query(self.model).update()

    def create_scoped_session(self, engine):
        Session = orm.sessionmaker(bind=engine)
        return orm.scoped_session(Session)

    def item_to_dict(self, item):
        d = vars(item)
        d.pop("_sa_instance_state")
        return d

    def create(self):
        #! TODO: create table from Pydantic
        self.model_orm.__table__.create(bind=self.session.bind)