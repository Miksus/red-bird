
.. _custom_repo:

Creating Custom Repository
==========================

Creating custom repositories can be done by simply 
sublcassing ``TemplateRepo``. ``TemplateRepo`` is 
a Pydantic model that is integrated with Red Bird
repository system.

In order to successfully create a custom repository,
the following methods must be implemented:

- ``insert(item)``: This method creates an item to the repository
- ``query_data(query)``: This method gets items from the repository accoding to ``query``
- ``query_update(query, values)``: This method updates items in the repository matching to ``query`` with values defined in ``values``
- ``query_delete(query)``: This method deletes items from the repository matching the given ``query``

In addition, the following methods could also be implemented for better performance:

- ``query_read_first(query)``: This method queries the first record from the data store from items matching the ``query`` 
- ``query_read_limit(query, n)``: This method queries the first ``n`` items from the data store from items matching the ``query`` 
- ``query_read_last(query)``: This method queries the last item from the data store from items matching the ``query``
- ``query_replace(query, values)``: This method replaces the found item(s) from the data store from items matching the ``query`` using the values given in ``values``
- ``query_count(query)``: This method counts the values from the data store from items matching the ``query``

The following methods could also be implemented for handling of the internals:

- ``item_to_data(item)``: This method transforms an instance of ``repo.model`` to data undestood by the underlying data store
- ``data_to_item(data)``: This method transforms raw data from the data store to an instance of ``repo.model``
- ``format_query(query)``: This method formats the ``query`` to a form suitable for the data store

Example
-------

To demonstrate the subclassing, first we create a simple example that 
only works with ``dict`` as a model and without supporting any complicated 
querying options:

.. code-block:: python

    from redbird.templates import TemplateRepo

    class MyRepo(TemplateRepo):

        store = [] # List that acts as our data store

        def insert(self, item):
            "Insert an item to the data store"
            self.store.append(item)

        def query_data(self, query):
            "Read data from the data store"
            for item in self.store:
                include_item = all(
                    query_value == item[key]
                    for key, query_value in query.items()
                )
                if include_item:
                    yield item
                        
        def query_update(self, query, values):
            "Update items in the data store"
            for item in self.store:
                update_item = all(
                    query_value == item[key]
                    for key, query_value in query.items()
                )
                if update_item:
                    for key, updated_value in values.items():
                        item[key] = updated_value
        
        def query_delete(self, query):
            "Delete items from the data store"
            for i, item in enumerate(self.store.copy()):
                delete_item = all(
                    query_value == item[key]
                    for key, query_value in query.items()
                )
                if delete_item:
                    del self.store[i]

Then we may use this repository as:

.. code-block:: python

    >>> repo = MyRepo()

    >>> # Creating some items
    >>> repo.add({'name': 'Jack', 'age': 30})
    >>> repo.add({'name': 'John', 'age': 30})
    >>> repo.add({'name': 'James', 'age': 40})

    >>> # Getting all items
    >>> repo.filter_by().all()
    [
        {'name': 'Jack', 'age': 30},
        {'name': 'John', 'age': 30},
        {'name': 'James', 'age': 40}
    ]

    >>> # Getting an item
    >>> repo.filter_by(age=30).first()
    {'name': 'Jack', 'age': 30}

    >>> # Getting some items depending on the query
    >>> repo.filter_by(age=30).all()
    [
        {'name': 'Jack', 'age': 30},
        {'name': 'John', 'age': 30}
    ]

    >>> # Updating some items
    >>> repo.filter_by(name='James').update(age=41)

    >>> # Deleting some items
    >>> repo.filter_by(age=30).delete()

Template Class
--------------

.. autoclass:: redbird.templates.TemplateRepo
    :members:
    :noindex:
