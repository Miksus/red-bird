
from copy import copy
import datetime
from functools import partial
import sys
from typing import TYPE_CHECKING, Callable, Dict, Iterable, List, Mapping, Optional, Tuple, Type, Union
from pathlib import Path
import typing

from redbird.oper import Between, In, Operation, skip
from redbird.packages import sqlalchemy, import_exists

from pydantic import BaseModel


try:
    from typing import Literal
except ImportError: # pragma: no cover
    from typing_extensions import Literal

if TYPE_CHECKING:
    from sqlalchemy.engine import Engine
    from sqlalchemy import Column

SINGULAR = Union[str, int, float, bool, datetime.datetime, datetime.date]

class _KeyInspector:
    # Utility for getitem, delitem and setitem
    def __init__(self, primary_keys, keys):
        self.primary_keys = primary_keys
        self.keys = keys

    def is_multi_item(self):
        return isinstance(self.keys, list)

    def is_specific(self):
        # Is specific item(s) and not ranges with slice or without
        n_primary_keys = len(self.primary_keys)
        if self.is_range():
            return False
        if self.is_multi_item():
            # Keys: [('a', 1), ('a', 2), ...]
            first_key = self.keys[0]
            levels = len(first_key) if not isinstance(first_key, (str,)) else 1
        elif isinstance(self.keys, str):
            levels = 1
        else:
            # Keys: ('a', 1)
            levels = len(self.keys)

        return levels == n_primary_keys

    def is_range(self):
        if isinstance(self.keys, tuple):
            return any(isinstance(key, slice) for key in self.keys)
        else:
            return isinstance(self.keys, slice)

    def _to_equal(self, column, value):
        # Compare column with value (which can be slice)
        if isinstance(value, slice):
            if value.step is not None:
                raise ValueError("Slice step not supported")

            if value.start is None and value.stop is None:
                return sqlalchemy.true()
            elif value.start is None:
                return column <= value.stop
            elif value.stop is None:
                return column >= value.start
            else:
                # Both not None
                return column.between(value.start, value.stop)
        return column == value

    def to_query(self):
        primary_keys = self.primary_keys
        keys = self.keys
        if len(primary_keys) == 0:
            raise TypeError("Table has no primary keys")

        if self.is_multi_item():
            # Fetch multiple items/ranges
            # Fetching multiple items
            qries = []

            n_levels = None
            keys: List[Union[Tuple, SINGULAR]]
            for item in keys:
                if not isinstance(item, tuple):
                    item = (item,)
                n_levels = len(item) if n_levels is None else n_levels
                if len(item) != n_levels:
                    raise IndexError(f"Key out of range")
                item_query = [
                    self._to_equal(primary_keys[i], key)
                    for i, key in enumerate(item)
                ]
                qries.append(sqlalchemy.and_(*item_query))
            qry = sqlalchemy.or_(*qries)
            return qry
        else:
            # Keys: ("a", 1) or "a"
            if not isinstance(keys, tuple):
                keys = (keys,)

            item_queries = []
            for i, key in enumerate(keys):
                item_queries.append(self._to_equal(primary_keys[i], key))
                if isinstance(key, (list, tuple)):
                    raise TypeError(f"Invalid index value type: {type(key)}")
            qry = sqlalchemy.and_(*item_queries)
            return qry


class Table:
    """SQL Table

    Similar to ``sqlalchemy.Table`` except this class always
    points to a specific SQL table in a specific database
    (which may not yet exist) and provides further abstraction.

    Attributes
    ----------
    name : str
        Name of the table.
    bind : sqlalchemy.engine.Connectable
        SQLAlchemy engine or connection.
    
    Examples
    --------
    .. code-block:: python

        from sqlalchemy import create_engine
        from redbird.sql import Table

        table = Table("mytable", bind=create_engine("sqlite://"))
    """
    bind: 'sqlalchemy.engine.Connectable'

    types = {
        str: sqlalchemy.String,
        int: sqlalchemy.Integer,
        float: sqlalchemy.Float,
        bool: sqlalchemy.Boolean,
        datetime.date: sqlalchemy.Date,
        datetime.datetime: sqlalchemy.DateTime,
        datetime.timedelta: sqlalchemy.Interval,
        dict: sqlalchemy.JSON,
    } if import_exists("sqlalchemy") else {}
    _name: str
    _object: 'sqlalchemy.Table'

    class _trans_ctx:
        # Utility for transaction context manager
        def __init__(self, obj:'Table'):
            self.obj = obj

        def __enter__(self):
            new_table = copy(self.obj)
            new_table._ctx = None
            self._ctx = new_table.bind.begin()
            new_table.bind = self._ctx.__enter__()
            return new_table

        def __exit__(self, type_, value, traceback):
            self._ctx.__exit__(type_, value, traceback)

    class _exec_ctx:
        # Utility for execution context manager
        def __init__(self, obj:'Table', args, kwargs):
            self.obj = obj
            self.args = args
            self.kwargs = kwargs

        def __enter__(self):
            bind = self.obj.bind

            # sqlalchemy.engine.Engine does not have "closed" attribute
            # but Connection does. We use this to define if it is open
            is_open = hasattr(bind, "closed") and not bind.closed
            if is_open:
                # "execute" should return Results that are still open
                return self.obj.execute(*self.args, **self.kwargs)
            else:
                self._conn = bind.connect()
                self._conn.__enter__()

                new_table = copy(self.obj)
                new_table.bind = self._conn

                return new_table.execute(*self.args, **self.kwargs)

        def __exit__(self, type_, value, traceback):
            if hasattr(self, "_conn"):
                self._conn.__exit__(type_, value, traceback)

    def __init__(self, name:str, bind:'sqlalchemy.engine.Engine'):
        self.bind = bind
        self.name = name

    def __getitem__(self, keys:Union[SINGULAR, Tuple[SINGULAR], List[Union[SINGULAR, Tuple[SINGULAR]]]]) -> Union[Dict, List[Dict]]:
        "Get item based on the index."
        prim_key_cols = self.object.primary_key.columns
        inspector = _KeyInspector(prim_key_cols, keys=keys)
        qry = inspector.to_query()
        results = list(self.select(qry))

        has_results = len(results) > 0
        is_specific = inspector.is_specific()
        if inspector.is_multi_item():
            if is_specific and not has_results:
                # Not all keys found
                raise KeyError("Missing key(s)")
            return results
        else:
            if not inspector.is_range() and not has_results:
                raise KeyError(f"Item {keys!r} not found")
            if is_specific:
                # Keys point to single item
                return results[0]
            else:
                # Keys point to range
                return results

    def __delitem__(self, keys):
        prim_key_cols = self.object.primary_key.columns
        inspector = _KeyInspector(prim_key_cols, keys)
        qry = inspector.to_query()
        nrows = self.delete(qry)
        if not inspector.is_range() and nrows == 0:
            raise KeyError("Item not found")

    def select(self, qry:Union[str, dict, 'sqlalchemy.sql.ClauseElement', None]=None, columns:Optional[List[str]]=None, parameters:Optional[Dict]=None) -> List[dict]:
        """Read the database table using a query.
        
        Parameters
        ----------
        qry : str, dict, sqlalchemy.sql.ClauseElement, optional
            Query to filter the data. The argument can take various forms:

            - ``str``: Query is considered to be raw SQL
            - ``dict``: Query is considered to be column-filter pairs.
              The pairs are combined using AND operator. If the filter
              is Operation, it is turned to corresponing SQLAlchemy expression.
              Else, the filter is considered to be an equal operator.
            - sqlalchemy expression: The query is considered to be the *where*
              clause of the select query. 

            If not given, all rows are returned.
        columns : list of string, optional
            List of columns to return. By default returns all columns.
        parameters : dict, optional
            Parameters for the query. If the query is as raw SQL,
            they should be set in the query with ``:myparam``, ie.
            ``select * from table where col = :myparam``.

        Returns
        -------
        List of dicts
            Found rows as dicts.

        Examples
        --------
        Select all rows:
        
        .. code-block:: python

            table.select()
        
        Select using raw SQL:
        
        .. code-block:: python

            table.select("SELECT * FROM mytable")

        Select using dictionary:

        .. code-block:: python

            table.select({"column_1": "a value", "column_2": 10})

        .. note::

            The above is same as:

            .. code-block:: sql
            
                SELECT * 
                FROM mytable 
                WHERE column_1 = 'a value' AND column_2 = 10

        Select using SQLAlchemy expressions:

        .. code-block:: python

            from sqlalchemy import Column
            table.select((Column("column_1") == "a value") & (Column("column_2") == 10))

        .. note::

            The above is same as:

            .. code-block:: sql
            
                SELECT * 
                FROM mytable 
                WHERE column_1 = 'a value' AND column_2 = 10

        Select and return specific column(s):

        .. code-block:: python

            table.select(columns=["column_1", "column_2"])

        .. note::

            The above is same as:

            .. code-block:: sql
            
                SELECT column_1, column_2 
                FROM mytable

        Select using raw strings and SQL parameters:

        .. code-block:: python

            table.select(
                "SELECT * FROM mytable WHERE column_1 = :myparam",
                parameters={"myparam": "a value"}
            )
        """
        if isinstance(qry, Path):
            qry = qry.read_text()
        elif qry is None:
            qry = sqlalchemy.true()

        if isinstance(qry, dict):
            qry = to_expression(qry)
        if isinstance(qry, str):
            statement = sqlalchemy.text(qry)
        elif isinstance(qry, (sqlalchemy.sql.Selectable, sqlalchemy.sql.elements.TextClause)):
            statement = qry
        else:
            if columns is None:
                columns = ()
            else:
                columns = [
                    self.object.columns[col]
                    for col in columns
                ]
            where = qry
            statement = self.object.select(*columns)
            statement = statement.where(where)
            statement = statement.select_from(self.object)

        if parameters is not None:
            statement = statement.bindparams(**parameters)

        with self.execution(statement) as results:
            rows = results.mappings()
            if self.name is not None:
                rows = self._format_results(rows)
            return list(rows)
    
    def insert(self, data:Union[Mapping, List[dict]]):
        """Insert data to the database.
        
        Parameters
        ----------
        data : dict, list of dicts
            Data to be inserted.

        Examples
        --------
        Insert a single row:
        
        .. code-block:: python

            table.insert({"column_1": "a", "column_2": 1})

        Insert multiple rows:
        
        .. code-block:: python

            table.insert([
                {"column_1": "a", "column_2": 1},
                {"column_1": "b", "column_2": 2},
            ])
        """
        table = self.object
        if isinstance(data, Mapping):
            statement = table.insert().values(**data)
            self.execute(statement)
        else:
            self.execute(sqlalchemy.insert(self.object), data)

    def delete(self, where:Union[dict, 'sqlalchemy.sql.ClauseElement']) -> int:
        """Delete row(s) from the table.
        
        Parameters
        ----------
        where : dict, sqlalchemy expression
            Where clause to delete data.

        Returns
        -------
        int
            Count of rows deleted.

        Examples
        --------

        Delete using dictionary:

        .. code-block:: python

            table.delete({"column_1": "a", "column_2": 1})

        .. note::

            The above is same as:

            .. code-block:: sql
            
                DELETE FROM mytable 
                WHERE column_1 = 'a' AND column_2 = 1

        Delete using SQL expressions:

        .. code-block:: python

            from sqlalchemy import Column
            table.delete((Column("column_1") == "a") & (Column("column_2") == 1))

        .. note::

            The above is same as:

            .. code-block:: sql
            
                DELETE FROM mytable 
                WHERE column_1 = 'a' AND column_2 = 1

        Delete all:

        .. code-block:: python

            table.delete({})

        """
        if isinstance(where, dict):
            where = to_expression(where)
        table = self.object
        statement = table.delete().where(where)
        result = self.execute(statement)
        return result.rowcount

    def update(self, where:Union[dict, 'sqlalchemy.sql.ClauseElement'], values:dict) -> int:
        """Update row(s) in the table.
        
        Parameters
        ----------
        where : dict, sqlalchemy expression
            Where clause to update rows.
        values : dict
            Column-value pairs to update the 
            rows matching the where clause.

        Returns
        -------
        int
            Count of rows updated.

        Examples
        --------

        Update using dicts:

        .. code-block:: python

            table.update({"column_1": "a", "column_2": 1}, {"column_3": "new value"})

        .. note::

            The above is same as:

            .. code-block:: sql
                
                UPDATE mytable 
                SET column_3='new value' 
                WHERE column_1 = 'a' and column_2 = 1

        Update using expressions:

        .. code-block:: python

            from sqlalchemy import Column
            table.update((Column("column_1") == "a") & (Column("column_2") == 1), {"column_3": "new value"})

        .. note::

            The above is same as:

            .. code-block:: sql
                
                UPDATE mytable 
                SET column_3='new value' 
                WHERE column_1 = 'a' and column_2 = 1

        Update all:

        .. code-block:: python

            table.update({}, {"column_3": "new value"})

        """
        if isinstance(where, dict):
            where = to_expression(where)
        table = self.object
        statement = table.update().where(where).values(values)
        result = self.execute(statement)
        return result.rowcount

    def count(self, where=None) -> int:
        """Count the number of rows.
        
        Parameters
        ----------
        where : dict, sqlalchemy expression, optional
            Where clause to be satisfied for counting the rows.

        Returns
        -------
        int
            Count of rows (satisfying the where clause).

        Examples
        --------

        Count based on dict:

        .. code-block:: python

            table.count({"column_1": "a", "column_2": 1})
        """
        stmt = sqlalchemy.select(sqlalchemy.func.count()).select_from(self.object)
        if where is not None:
            if isinstance(where, dict):
                where = to_expression(where)
            stmt = stmt.where(where)

        with self.execution(stmt) as results:
            return results.scalar_one()

    def _format_results(self, res:Iterable[Tuple]) -> Iterable[dict]:
        columns = self._get_types()
        for row in res:
            row = {name: conv(row[name]) for name, conv in columns.items()}
            yield row

    def _get_types(self) -> Dict[str, Callable]:
        "Get table's column types"
        cols = dict(self.object.columns)
        return {col_name: partial(to_native, sql_type=col.type, nullable=col.nullable) for col_name, col in cols.items()}

    def _to_sqlalchemy_type(self, cls):
        from sqlalchemy.sql import sqltypes
        if isinstance(cls, sqltypes.TypeEngine):
            return cls
        is_older_py = sys.version_info < (3, 8)
        origin = typing.get_origin(cls) if not is_older_py else None
        if origin is not None:
            # In form: 
            # - Literal['', '']
            # - Optional[...]
            args = typing.get_args(cls)
            if origin is typing.Union:
                # Either:
                # - Union[...]
                # - Optional[...]
                # Only Union[<TYPE>, NoneType] is allowed
                none_type = type(None)
                has_none_type = none_type in args
                if len(args) > 2 or (len(args) == 2 and not has_none_type):
                    raise TypeError(f"Union has more than one optional type: {str(cls)}. Cannot define SQL data type")
                # Get the non-None type
                for arg in args:
                    if arg is not none_type:
                        cls = arg
                        break
            
            if origin is Literal:
                type_ = type(args[0])
                for arg in args[1:]:
                    if not isinstance(arg, type_):
                        raise TypeError(f"Literal values are not same types: {str(cls)}. Cannot define SQL data type")
                cls = type_
        return self.types.get(cls)

    def reflect(self):
        """Reflect the table from the database."""
        self._object = reflect_table(self.name, bind=self.bind)

    def drop(self):
        """Drop the table."""
        self.object.drop(bind=self.bind)

    def exists(self) -> bool:
        """Check if the table exists."""
        inspector = sqlalchemy.inspect(self.bind)
        return inspector.has_table(self.name)

    def create(self, columns:Union[List['Column'], Mapping[str, Type]], exist_ok=False):
        """Create the table
        
        Parameters
        ----------
        columns : dict, list of sqlalchemy.Column, dict or string
            Columns to be created.
        exist_ok : bool
            If false (default), an exception is raised.

        Examples
        --------

        There are various ways to call this method:

        - Using list of columns (all of the)

        Create a table with columns ``column_1``, ``column_2`` and ``column_3``
        (all of them are textual columns): 

        .. code-block:: python

            table.create(["column_1", "column_2", "column_3"])

        Create a table with columns ``column_1``, ``column_2`` and ``column_3``
        with varying data types: 

        .. code-block:: python

            import datetime
            table.create({"column_1": str, "column_2": int, "column_3": datetime.datetime})

        Create a table with columns ``column_1``, ``column_2`` and ``column_3``
        using SQLAlchemy columns: 

        .. code-block:: python

            from sqlalchemy import Column, String, Integer, DateTime
            table.create([
                Column("column_1", type_=String()),
                Column("column_2", type_=Integer()),
                Column("column_3", type_=DateTime())
            ])

        Create a table with columns ``column_1``, ``column_2`` and ``column_3``
        using list of dicts: 

        .. code-block:: python

            from sqlalchemy import DateTime
            table.create([
                {"name": "column_1", "type_": str},
                {"name": "column_2", "type_": int},
                {"name": "column_3", "type_": DateTime()},
            ])
        """
        if isinstance(columns, Mapping):
            columns = [
                sqlalchemy.Column(name, self._to_sqlalchemy_type(type_))
                for name, type_ in columns.items()
            ]
        elif isinstance(columns, (list, tuple)):
            columns = [
                sqlalchemy.Column(col, type_=sqlalchemy.String()) if isinstance(col, str) 
                else sqlalchemy.Column(
                    type_=self._to_sqlalchemy_type(col['type_']), 
                    **{k: v for k, v in col.items() if k not in ('type_',)}
                ) if isinstance(col, dict)
                else col
                for col in columns
            ]
        meta = sqlalchemy.MetaData()
        tbl = sqlalchemy.Table(self.name, meta, *columns)

        if exist_ok and self.exists():
            self._object = tbl
            return

        tbl.create(bind=self.bind)
        self._object = tbl

    def create_from_model(self, model:BaseModel, primary_column=None):
        if isinstance(primary_column, str):
            primary_column = (primary_column,)
        sql_cols = [
            sqlalchemy.Column(
                name, 
                self._to_sqlalchemy_type(field.type_), 
                primary_key=name in primary_column if primary_column is not None else False, 
                nullable=not field.required,
                default=field.default
            )
            for name, field in model.__fields__.items()
        ]
        self.create(sql_cols)
  
    def execute(self, *args, **kwargs):
        """Execute SQL statement or raw SQL.
        
        Parameters
        ----------
        *args : tuple
            Passed directly to sqlalchemy.Connection.execute.
        **args : dict
            Passed directly to sqlalchemy.Connection.execute.
        """
        if len(args) == 1 and isinstance(args[0], str):
            # SQLAlchemy v2.0 won't accept string as 
            # expression but we do
            args = (sqlalchemy.text(*args),)
        conn = self.bind
        if isinstance(conn, sqlalchemy.engine.Engine):
            with conn.begin() as conn:
                return conn.execute(*args, **kwargs)
        else:
            return conn.execute(*args, **kwargs)

    def open_transaction(self):
        """Open a transaction.
        
        Examples
        --------
        .. code-block:: python

            from redbird.sql import Table
            from sqlalchemy import create_engine

            table = Table("mytable", bind=create_engine(...))

            # Open the transaction
            transaction = table.open_transaction()

            # Perform operations
            transaction.insert({"col_1": "a", "col_2": "b"})
            transaction.delete({"col_2": "c"})

            # Commit or rollback the changes
            if successful:
                transaction.commit()
            else:
                transaction.rollback()
        """
        self._ctx = self._trans_ctx(self)
        return self._ctx.__enter__()

    def transaction(self):
        """Open a transaction context.
        
        If an error is raised inside the with block,
        the changes are rollbacked. Else they are 
        commited.

        Examples
        --------
        .. code-block:: python

            from redbird.sql import Table
            from sqlalchemy import create_engine

            table = Table("mytable", bind=create_engine(...))

            with table.transaction() as trans:

                # Perform operations
                trans.insert({"col_1": "a", "col_2": "b"})
                trans.delete({"col_2": "c"})
        """
        return self._trans_ctx(self)

    def rollback(self):
        "Rollback the open transaction (a transaction must be open)"
        return self.bind.get_transaction().rollback()

    def commit(self):
        "Commit the open transaction (a transaction must be open)"
        return self.bind.get_transaction().commit()

    def execution(self, *args, **kwargs):
        "Context manager to manipulate the execution result"
        return self._exec_ctx(self, args, kwargs)

    @property
    def object(self) -> 'sqlalchemy.Table':
        "sqlalchemy.Table: SQLAlchemy representation of the table"
        if self._object is None:
            self.reflect()
        return self._object

    @object.setter
    def object(self, value: 'sqlalchemy.Table'):
        self._object = value
        self._name = value.name

    @property
    def name(self) -> str:
        "str: name of the table"
        if self._object is not None:
            return self._object.name
        return self._name
    
    @name.setter
    def name(self, value: str):
        self._name = value
        self._object = None


def to_native(value, sql_type, nullable=False):
    "Convert sql type to Python native."
    py_type = sql_type.python_type
    if isinstance(value, py_type):
        return value
    elif py_type is datetime.datetime and isinstance(value, str):
        return datetime.datetime.fromisoformat(value)
    elif py_type is datetime.date and isinstance(value, str):
        return datetime.date.fromisoformat(value)
    if nullable and value is None:
        return value
    return py_type(value)

def create_table(*args, bind:'Engine', table:str, **kwargs):
    """Create a table to a SQL database.
    
    Parameters
    ----------
    bind : sqlalchemy.engine.Engine
        SQLAlchemy engine for the connection
    table : str
        Name of the table to be created.
    *args
        Passed to :py:meth:`redbird.sql.Table.create`
    **kwargs
        Passed to :py:meth:`redbird.sql.Table.create`
    """
    return Table(bind=bind, name=table).create(*args, **kwargs)

def reflect_table(table:str, *columns, bind:'Engine', meta=None):
    """Reflect a table in an SQL database"""
    if meta is None:
        meta = sqlalchemy.MetaData()
    return sqlalchemy.Table(table, meta, *columns, autoload_with=bind)

def select(*args, bind:'Engine', table:str=None, **kwargs):
    """Read rows from a table in a SQL database.
    
    Parameters
    ----------
    engine : sqlalchemy.engine.Engine
        SQLAlchemy engine for the connection
    table : str
        Name of the table to use.
    *args
        Passed to :py:meth:`redbird.sql.Table.select`
    **kwargs
        Passed to :py:meth:`redbird.sql.Table.select`
    """
    return Table(bind=bind, name=table).select(*args, **kwargs)

def insert(*args, bind:'Engine', table:str=None, **kwargs):
    """Insert row(s) to a table in a SQL database.

    Parameters
    ----------
    bind : sqlalchemy.engine.Engine
        SQLAlchemy engine for the connection
    table : str
        Name of the table to use.
    *args
        Passed to :py:meth:`redbird.sql.Table.insert`
    **kwargs
        Passed to :py:meth:`redbird.sql.Table.insert`
    """
    return Table(bind=bind, name=table).insert(*args, **kwargs)

def delete(*args, bind:'Engine', table:str=None, **kwargs):
    """Delete row(s) in a table in a SQL database.
    
    Parameters
    ----------
    bind : sqlalchemy.engine.Engine
        SQLAlchemy engine for the connection
    table : str
        Name of the table to use.
    *args
        Passed to :py:meth:`redbird.sql.Table.delete`
    **kwargs
        Passed to :py:meth:`redbird.sql.Table.delete`
    """
    return Table(bind=bind, name=table).delete(*args, **kwargs)

def update(*args, bind:'Engine', table:str=None, **kwargs):
    """Update row(s) to a table in a SQL database.
    
    Parameters
    ----------
    bind : sqlalchemy.engine.Engine
        SQLAlchemy engine for the connection
    table : str
        Name of the table to use.
    *args
        Passed to :py:meth:`redbird.sql.Table.update`
    **kwargs
        Passed to :py:meth:`redbird.sql.Table.update`
    """
    return Table(bind=bind, name=table).update(*args, **kwargs)

def count(*args, bind:'Engine', table:str=None, **kwargs):
    """Count the number of rows in a table in a SQL database.

    Parameters
    ----------
    bind : sqlalchemy.engine.Engine
        SQLAlchemy engine for the connection
    table : str
        Name of the table to use.
    *args
        Passed to :py:meth:`redbird.sql.Table.count`
    **kwargs
        Passed to :py:meth:`redbird.sql.Table.count`
    """
    return Table(bind=bind, name=table).count(*args, **kwargs)

def execute(*args, bind:'Engine', **kwargs):
    """Execute raw SQL or a SQL expression in a SQL database.

    Parameters
    ----------
    bind : sqlalchemy.engine.Engine
        SQLAlchemy engine for the connection
    table : str
        Name of the table to use.
    *args
        Passed to :py:meth:`redbird.sql.Table.execute`
    **kwargs
        Passed to :py:meth:`redbird.sql.Table.execute`
    """
    return Table(bind=bind, name=None).execute(*args, **kwargs)


def to_expression(qry:dict, table=None):
    stmt = sqlalchemy.true()
    for column_name, oper_or_value in qry.items():
        column = getattr(table, column_name) if table is not None else sqlalchemy.Column(column_name)
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
        elif isinstance(oper_or_value, slice):
            start, stop, step = oper_or_value.start, oper_or_value.stop, oper_or_value.step
            if step is not None:
                raise ValueError("Step cannot be interpret")
            if start is not None and stop is not None:
                sql_oper = column.between(start, stop)
            elif start is not None:
                sql_oper = column >= start
            elif stop is not None:
                sql_oper = column <= stop
            else:
                sql_oper = sqlalchemy.true()
        else:
            value = oper_or_value
            sql_oper = column == value
        stmt &= sql_oper
    return stmt