
from redrepo import BaseRepo, BaseResult
from sqlalchemy import Column, column, orm, true
from sqlalchemy.sql import True_

from redrepo.operation import Operation

class SQLAlchemyResult(BaseResult):

    repo: 'SQLAlchemyRepo'

    def query(self):
        model = self.repo.cls_item
        session = self.repo.session
        return session.query(model).filter(self.query_)

    def first(self):
        return self.query().first()

    def update(self, **kwargs):
        "Update the resulted rows"
        model = self.repo.cls_item
        session = self.repo.session

        session.query(model).filter(self.query_).update(kwargs)

    def delete(self):
        model = self.repo.cls_item
        session = self.repo.session
        session.query(model).filter(self.query_).delete()

    def count(self):
        model = self.repo.cls_item
        session = self.repo.session
        return session.query(model).filter(self.query_).count()

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
        model = self.repo.cls_item
        return 

class SQLAlchemyRepo(BaseRepo):
    """SQLAlchemy Repository

    Examples
    --------
        .. code-block: python

            repo = Messages(model)
            msg = repo["my_msg_id"]
    """
    cls_result = SQLAlchemyResult

    def __init__(self, cls_item, engine, id_field=None):
        self.cls_item = cls_item
        self.session = self.create_scoped_session(engine)
        self.id_field = id_field or self.default_id_field

    def delete_by(self, **kwargs):
        model = self.cls_item
        session = self.session
        return session.query(model).filter_by(**kwargs).delete()

    def add(self, item):
        self.session.add(item)
        self.session.commit()

    #def update(self, item):
    #    self.session.query(self.cls_item).update()

    def create_scoped_session(self, engine):
        Session = orm.sessionmaker(bind=engine)
        return orm.scoped_session(Session)

    def item_to_dict(self, item):
        d = vars(item)
        d.pop("_sa_instance_state")
        return d

    def create(self):
        self.cls_item.__table__.create(bind=self.session.bind)