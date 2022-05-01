.. _tutorial:

Tutorial
========

This section covers basic tutorials of 
Red Bird.

Installation
------------

Install the package:

.. code-block:: console

    pip install redbird

See `PyPI for Red Bird releases <https://pypi.org/project/redbird/>`_.

Configuring Repository
----------------------

The full list of built-in repositories and their examples are found from 
`repository section <repositories>`. Below is a simple example to 
configure in-memory repository.

.. code-block:: python

    from redbird.ext import MemoryRepo
    repo = MemoryRepo()

By default, the items are manipulated as dictionaries. You may also create a 
Pydantic model in order to have better data validation and control over 
the structure of the items:

.. code-block:: python

    from pydantic import BaseModel

    class Car(BaseModel):
        registration_number: str
        color: str
        value: float

    from redbird.ext import MemoryRepo
    repo = MemoryRepo(model=Car)

See more about configuring repositories from :ref:`here <repositories>`.

Usage Examples
--------------

Create operation:

.. code-block::

    # If you use dict as model
    repo.add({"registration_number": "123-456-789", "color": "red"})

    # If you Pydantic model:
    repo.add(Car(registration_number="111-222-333", color="red"))
    repo.add(Car(registration_number="444-555-666", color="blue"))

Get operation:

.. code-block::

  # One item
  repo["123-456-789"]

  # Multiple items
  repo.filter_by(color="red").all()

Update operation:

.. code-block::

  # One item
  repo["123-456-789"] = {"condition": "good"}

  # Multiple items
  repo.filter_by(color="blue").update(color="green")

Delete operation:

.. code-block::

  # One item
  del repo["123-456-789"]

  # Multiple items
  repo.filter_by(color="red").delete()
