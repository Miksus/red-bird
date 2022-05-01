

CRUD: Create, Read, Update, Delete 
==================================

The idea of Red Bird is to have unified syntax 
regardless of the data store. This section
contains repository methods that work indentically
regardless of the repository. However, there may
be some exceptions in special repositories.

In order to use the CRUD operations, remember 
to set the repository:

.. code-block:: python

    repo = Repo(...)

And the item:

.. code-block:: python

    from pydantic import BaseModel

    class Person(BaseModel):
        id: str
        name: str
        nationality: str

.. _create:

Create
------

To create an item/record to the repository:

.. code-block:: python

    repo.add(Person(id="11-11-11", name="Jack", nationality='British'))

.. _read:

Read
----

You may get all the items from the repository by simply
iterating over it:

.. code-block:: python

    list(repo)

You may also get items that contain given attribute values:

.. code-block:: python

    repo.filter_by(nationality="Finnish").all()

If you are only interested in the first found, you may 
use ``first``:

.. code-block:: python

    repo.filter_by(nationality="Finnish").first()

If you are only interested in the last found, you may 
use ``last``:

.. code-block:: python

    repo.filter_by(nationality="Finnish").last()

You may also fetch the first n found items:

.. code-block:: python

    repo.filter_by(nationality="Finnish").limit(2)

If you have ``id_field`` specified in the repository,
you may also get an item using the ID by using any of 
the following:

.. code-block:: python

    repo["11-22-33"]
    repo.get_by("11-22-33").first()

.. _update:

Update
------

In order to update an item in a repository, use ``update``
and pass the updated item:

.. code-block:: python

    person = repo["11-11-11"]
    person.age += 1
    repo.update(person)

You may also update several at a time:

.. code-block:: python

    repo.filter_by(nationality="English").update(age=30)

You may also update an item by the ID field using ``get_by``:

.. code-block:: python

    repo.get_by("11-22-33").update(age=30)

.. _delete:

Delete
------

To delete an item, use ``del``:

.. code-block:: python

    del repo["11-11-11"]

or you may also use ``delete`` method:

.. code-block:: python

    person = repo["22-22-22"]
    repo.delete(person)

You may also delete multiple items:

.. code-block:: python

    repo.filter_by(nationality="English").delete()

You may also delete an item by the ID field using ``get_by``:

.. code-block:: python

    repo.get_by("11-22-33").delete()