
from redbird.oper import Operation

class QueryParser:

    def format_query(self, query:dict) -> dict:
        "Turn the query to a form that's understandable by the underlying database"
        for field_name, oper_or_value in query.copy().items():
            if isinstance(oper_or_value, Operation):
                query[field_name] = self.format_operation(oper_or_value)
        return query

    def format_operation(self, oper:Operation):
        result_format_method = oper._get_formatter(self)
        return result_format_method(oper)


class StringQuery(QueryParser):
    "Basic string query parser"
    def __init__(self, case="camel"):
        self.case = case

class PythonQuery(QueryParser):
    "Query for Python objects"

    def format_query(self, query:dict):
        return query


class SQLAlchemyQuery(QueryParser):

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

class MongoQuery(QueryParser):

    def format_greater_than(self, oper:Operation):
        return {"$gt": oper.value}

    def format_less_than(self, oper:Operation):
        return {"$lt": oper.value}

    def format_greater_equal(self, oper:Operation):
        return {"$gte": oper.value}

    def format_less_equal(self, oper:Operation):
        return {"$lte": oper.value}

    def format_not_equal(self, oper:Operation):
        return {"$ne": oper.value}


class HTTPQuery(QueryParser):

    def format_query(self, query: dict, repo) -> str:
        query = super().format_query(query)

        url_params = self.url_params.copy()
        if query is not None:
            url_params.update(query)
        id = url_params.pop(self.id_field) if query is not None and self.id_field in query else None

        url_base = self.url
        url_params = urlparse.urlencode(url_params) # Turn {"param": "value"} --> "param=value"

        if id is None:
            id = ""
        elif not id.startswith("/"):
            id = "/" + id
        if url_params:
            url_params = "?" + url_params

        # URL should look like "www.example.com/api/items/{id}?{param}={value}"
        # or "www.example.com/api/items/{id}"
        # or "www.example.com/api/items?{param}={value}"
        # or "www.example.com/api/items"
        return f"{id}{url_params}"