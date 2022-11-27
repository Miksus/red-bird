.. _repositories:

Repositories
============

This section consists of configuring various built-in repositories.
After configuration, the usage of the repositories should be identical
for all features except those stated otherwise.

.. toctree::
   :maxdepth: 1
   :caption: Contents:

   memory
   csv
   json_dir
   sql
   mongo

   custom_repo

Common Configurations
---------------------

All repositories support some common configurations (unless specified
otherwise). The item type can be either dict or Pydantic model and 
you may also specify which field is used as the identification field.

Specifying the Item Model
^^^^^^^^^^^^^^^^^^^^^^^^^

The item model can be either dict or Pydantic model.
If you wish to have dynamic items in the sense that 
the items may have arbitrary number of fields with 
arbitrary type you may just use dict as the model:

.. code-block:: python

    repo = MemoryRepo(model=dict, ...)

However, in most cases it is recommended to specify 
the item model as a Pydantic model. By doing so,
the structure of each item is better documented 
and you may leverage the extensive data validation
Pydantic offers.

.. code-block:: python

    from pydantic import BaseModel

    class Car(BaseModel):
        registration_number: str
        color: str
        value: float

    from redbird.repos import MemoryRepo
    repo = MemoryRepo(model=Car, ...)

Specifying the ID Field
^^^^^^^^^^^^^^^^^^^^^^^

Most repositories also support ID fields. ID field is a 
field/key/attribute of the items that is unique for 
all and is 

.. code-block:: python

    repo = MemoryRepo(id_field="registration_number", ...)

Alternatively, the ``id_field`` could be set using ``__id_field__`` magic attribute:

.. code-block:: python

    from pydantic import BaseModel

    class Car(BaseModel):
        __id_field__ = "registration_number"
        registration_number: str
        color: str
        value: float

    repo = MemoryRepo(model=Car)

Specifying ID field is necessary for some features to work
such as:

- getitem: :code:`repo['123-456-789']`
- delitem: :code:`del repo['123-456-789']`
- get_by: :code:`repo.get_by('123-456-789')`
