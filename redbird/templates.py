
from abc import abstractmethod
from typing import Any, ClassVar, Dict, Iterator, List, Union
from .base import BaseRepo, BaseResult, Data, Item

class TemplateResult(BaseResult):
    repo: 'TemplateRepo'

    def query_data(self) -> Iterator[Data]:
        "Get actual result"
        return self.repo.query_read(self.query_)

    def update(self, **kwargs):
        return self.repo.query_update(self.query_, kwargs)

    def delete(self):
        return self.repo.query_delete(self.query_)

    def replace(self, __values:dict=None, **kwargs):
        "Replace the existing item(s) with given"
        if __values is not None:
            kwargs.update(__values)
        try:
            return self.repo.query_replace(self.query_, kwargs)
        except NotImplementedError:
            # Has no custom replace implemented, using 
            # default alternative (delete, add)
            return super().replace(**kwargs)

    def count(self) -> int:
        try:
            return self.repo.query_count(self.query_)
        except NotImplementedError:
            # Has no custom replace implemented, using 
            # default alternative (len(self))
            return super().count()

    def first(self) -> Item:
        try:
            return self.repo.query_read_first(self.query_)
        except NotImplementedError:
            return super().first()

    def limit(self, n:int) -> List[Item]:
        try:
            return self.repo.query_read_limit(self.query_, n)
        except NotImplementedError:
            return super().limit(n)

    def last(self) -> Item:
        try:
            return self.repo.query_read_last(self.query_)
        except NotImplementedError:
            return super().last()

    def format_query(self, query: dict) -> dict:
        qry = super().format_query(query)
        try:
            return self.repo.format_query(qry)
        except NotImplementedError:
            return qry


class TemplateRepo(BaseRepo):
    """Template repository for easy subclassing

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
        the item inn case of validation error in 
        converting data to the item model from
        the repository. By default raise 

    Examples
    --------

    .. code-block:: python

        class MyRepo(TemplateRepo):

            def insert(self, item):
                # Insert item to the data store
                ...

            def query_read(self, query: dict):
                # Get data from repository
                for item in ...: 
                    yield item

            def query_update(self, query: dict, values: dict):
                # Update items with values matcing the query
                ...

            def query_delete(self, query: dict):
                # Delete items matcing the query
                ...

            def item_to_data(self, item):
                # Convert item to type that is understandable
                # by the repository's data store
                ...
                return data
    """
    cls_result: ClassVar = TemplateResult

    @abstractmethod
    def query_read(self, query):
        """Query all

        Override this method.

        Parameters
        ----------
        query : any
            Repository query, by default dict.

        Examples
        --------
        .. code-block:: python

            repo.filter_by(color="red").all()
        
        .. warning::

            This class is meant to be called directly.
        """
        ...

    @abstractmethod
    def query_update(self, query, values):
        """Update the result of the query with given values

        Override this method.

        Parameters
        ----------
        query : any
            Repository query, by default dict.

        Examples
        --------
        .. code-block:: python

            repo.filter_by(color="red").update(color="blue")

        """
        ...

    @abstractmethod
    def query_delete(self, query):
        """Delete the result of the query

        Override this method.

        Parameters
        ----------
        query : any
            Repository query, by default dict.

        Examples
        --------
        .. code-block:: python

            repo.filter_by(color="red").delete()

        """
        ...

    def query_read_first(self, query: dict) -> Item:
        """Query the first item

        You may override this method. By default,
        the first item returned by :class:`TemplateRepo.query_read`
        is returned.

        Parameters
        ----------
        query : any
            Repository query, by default dict.

        Examples
        --------
        .. code-block:: python

            repo.filter_by(color="red").first()

        """
        raise NotImplementedError("Read using first not implemented.")

    def query_read_limit(self, query: dict, n:int) -> List[Item]:
        """Query the first n items

        You may override this method. By default,
        the N first items returned by :class:`TemplateRepo.query_read`
        are returned.

        Parameters
        ----------
        query : any
            Repository query, by default dict.
        n : int
            Number of items to return

        Examples
        --------
        .. code-block:: python

            repo.filter_by(color="red").limit(3)

        """
        raise NotImplementedError("Read using limit not implemented.")

    def query_read_last(self, query: dict) -> Item:
        """Query the last item

        You may override this method. By default,
        the last item returned by :class:`TemplateRepo.query_read`
        is returned.

        Parameters
        ----------
        query : any
            Repository query, by default dict.

        Examples
        --------
        .. code-block:: python

            repo.filter_by(color="red").last()

        """
        raise NotImplementedError("Read using first not implemented.")

    def query_replace(self, query: dict, values: dict):
        """Replace the items with given values using given query

        You may override this method. By default,
        the result of the query is deleted and an
        item from the values is generated.

        Parameters
        ----------
        query : any
            Repository query, by default dict.
        values : dict
            Values to replace the items' existing
            values with

        Examples
        --------
        .. code-block:: python

            repo.filter_by(color="red").replace(color="blue")

        """
        raise NotImplementedError("Replacing using query not implemented.")

    def query_count(self, query: dict):
        """Count the items returned by the query

        You may override this method. By default,
        the items returned by :class:`TemplateRepo.query_read`
        are counted.

        Parameters
        ----------
        query : any
            Repository query, by default dict.

        Examples
        --------
        .. code-block:: python

            repo.filter_by(color="red").count()

        """
        raise NotImplementedError("Count using query not implemented.")

    def format_query(self, query: dict) -> dict:
        """Format the query to a format suitable by the repository

        You may override this method. By default,
        the query is as dictionary.

        Parameters
        ----------
        query : dict
            Query to reformat

        Examples
        --------
        .. code-block:: python

            repo.filter_by(color="red")

        """
        raise NotImplementedError("Query formatting not implemented.")
