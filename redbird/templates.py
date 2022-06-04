
from abc import abstractmethod
import warnings
from typing import Any, ClassVar, Dict, Iterator, List, Union

from redbird.exc import ConversionWarning, _handle_conversion_error
from .base import BaseRepo, BaseResult, Data, Item

class TemplateResult(BaseResult):
    repo: 'TemplateRepo'

    def query(self) -> Iterator[Item]:
        "Get actual result"
        try:
            items = self.repo.query_items(self.query_)
        except NotImplementedError:
            for data in self.repo.query_data(self.query_):
                try:
                    yield self.repo.data_to_item(data)
                except ValueError:
                    _handle_conversion_error(self.repo, data)
        else:
             yield from items

    def query_data(self) -> Iterator[Data]:
        "Get actual result"
        return self.repo.query_data(self.query_)

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

    def query_data(self, query: dict) -> Iterator[Data]:
        """Query (read) the data store and return raw data

        Override this or :func:`~redbird.templates.TemplateRepo.query_items` method.

        Parameters
        ----------
        query : dict
            Repository query, by default dict.

        Returns
        -------
        iterable (any)
            Iterable of raw data that is converted to an item using :func:`~redbird.base.BaseRepo.data_to_item`

        Examples
        --------

        Used in following cases:

        .. code-block:: python

            repo.filter_by(color="red").all()
        
        """
        raise NotImplementedError("Method query_data not implemented")

    def query_items(self, query: dict) -> Iterator[Item]:
        """Query (read) the data store and return items

        Override this or :func:`~redbird.templates.TemplateRepo.query_data` method.

        Parameters
        ----------
        query : dict
            Repository query, by default dict.

        Returns
        -------
        iterable (``model``)
            Items that are instances of the class in the ``model`` attibute.
            Typically dicts or instances of subclassed Pydantic BaseModel

        Examples
        --------

        Used in following cases:

        .. code-block:: python

            repo.filter_by(color="red").all()
        
        """
        raise NotImplementedError("Method query_items not implemented")

    @abstractmethod
    def query_update(self, query: dict, values: dict):
        """Update the result of the query with given values

        Override this method.

        Parameters
        ----------
        query : any
            Repository query, by default dict.

        Examples
        --------

        Used in following cases:

        .. code-block:: python

            repo.filter_by(color="red").update(color="blue")

        """
        ...

    @abstractmethod
    def query_delete(self, query: dict):
        """Delete the result of the query

        Override this method.

        Parameters
        ----------
        query : any
            Repository query, by default dict.

        Examples
        --------

        Used in following cases:

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

        Used in the following case:

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

        Used in the following case:

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

        Used in the following case:

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

        Used in the following case:

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

        Used in the following case:

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

        Used in the following case:
        
        .. code-block:: python

            repo.filter_by(color="red")

        """
        raise NotImplementedError("Query formatting not implemented.")
