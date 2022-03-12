
from redrepo import BaseRepo, BaseResult
from sqlalchemy import orm

class SQLAlchemyResult(BaseResult):

    repo: 'SQLAlchemyRepo'

    def query(self):
        model = self.repo.cls_item
        session = self.repo.session
        return session.query(model).filter_by(**self.query_)

    def first(self):
        return self.query().first()

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

    def update(self, item):
        self.session.query(self.cls_item).update()

    def create_scoped_session(self, engine):
        Session = orm.sessionmaker(bind=engine)
        return orm.scoped_session(Session)

    def create(self):
        self.cls_item.__table__.create(bind=self.session.bind)