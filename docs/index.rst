
.. meta::
   :description: Red Bird is a repository pattern library for Python.
   :keywords: repository, pattern, Python, database

Red-Bird: Repository Patterns for Python
========================================

Repository pattern is a technique to abstract the data access from
the domain/business logic. In other words, it decouples the database 
access from the application code. The aim is that the code runs the 
same regardless if the data is stored to an SQL database, NoSQL 
database, file or even as an in-memory list.

- `Source code <https://github.com/Miksus/red-bird>`_
- `Releases (PyPI) <https://pypi.org/project/redbird>`_

Why Repository Pattern?
-----------------------

**Short Answer:** 

Because it simplifies things, makes prototyping
faster and testing easier.

**Long answer:**

Typically, data access in an application looks like this:

.. image:: /imgs/typical_application.png
    :align: center

In other words, querying the databases is embedded with the application
code, ie. the application code executes raw SQL, MongoDB queries or 
HTTP requests to APIs. This makes the application code to be data 
store specific. In order for the application to function, it 
must be connected to a specific type of a database. 

For many projects this approach may not cause additional difficulities but 
there are several problems with this approach related to readability and 
maintainability:

- Application code is data store specific thus later switching to another 
  data store may require a lot of work.
- Testing the application code is non-trivial if there is no test database 
  of the same type as the production.
- Understanding the application requires understanding how the underlying
  database works.
- The code may become hard to read if multiple types of databases are used,
  ie. SQL databases and MongoDB databases.

Repository pattern aims to separate the domain layer (application logic) from 
the database layer (data access) by unifying the syntax for creating, fetching,
modifying and deleting data in the data stores. It transforms generic actions
to the language a specific database understands.

In practice, this is illustrated below:

.. image:: /imgs/repository_pattern.png
    :align: center

The repositories act as translators to transform generic actions (read, 
create, update, delete data) to language the specific database understands.
The repositories may be configured at the configuraitons of the application
and they may easily be swapped to different database servers operating on 
the same or different querying language or data types. The application code 
is identical regardless if the data is in an SQL database, MongoDB or in-memory
lists. 


This has several benefits:

- Unit testing is easy as the repositories could be swapped to in-memory lists
- Database migrations is trivial as it require no code changes
- Using different databases or types of databases adds no additional maintenance costs
- Creating separate environments operating on different connections is easy.

However, there are some downsides with repository pattern as well. Most notably,
some features in querying languages cannot be replicated in a way that works
with all others simply due to that these features are missing in them. Especially,
some optimizations are such that cannot be replicated. However, most applications 
don't require optimized or complex queries. Furthermore, applications that 
do need them may still implement repository pattern and use database specific
queries only in places where this is unavoidable.

Main Features
-------------

In short, Red Bird offers:

- Identical way accross data stores of doing the following operations:

    - Create an item to the data store
    - Read items from the data store
    - Update items in the data store
    - Delete items in the data store

- Data validation via Pydantic
- Basic querying operations (ie. equal, greater or less than)
- :ref:`A log handler <logging_handler>` for logging to data stores

Supported repositories:

- SQL (via SQLAlchemy)
- MongoDB (via Pymongo)
- In-memory (objects in Python list)
- CSV (each row is an item)
- JSON (a JSON file per item)

Terminology
-----------

Unit of Work
^^^^^^^^^^^^

In Red Bird, the term *repository* is used to describe a 
specific data store that is a collection of items of the
same type (ie. a list of cars) and *item* is used to describe a record or 
a document with some attributes (ie. a car with registration number 123-456-789).
An attribute consists of the name of the attribute (ie. car color) and its 
value for specific item (ie. color of the car with registration number 123-456-789 is red).


The definition of these terms between various
data stores are illustrated below:

============== ===================== ================ =================================
Repository     Repository definition Item definition  Attribute definition
============== ===================== ================ =================================
Python memory  list                  object           attribute (object) or item (dict)
SQL            table                 row              column
MongoDB        collection            document         field
REST (HTTP)    URL endpoint          JSON object      field
============== ===================== ================ =================================

Database Operations
^^^^^^^^^^^^^^^^^^^

There are four generic operations implemented by almost every data store:
create, read, update and delete. These are often labeled CRUD. Read-only
data store may only have read operation and some simple data stores may 
only have read, create and delete (such as a CSV file). For basic CRUD 
actions, Red Bird uses tems more commonly used in Python language: 
add, get, update and delete, respectively. Red bird also adds one more 
operation that is implemented in some data stores and could be implemented
to others by combining existing operations: replace. Replace is an update
but it also removes attributes that were not changed.

These unified methods for manipulating data (a single item) are illustrated below:

============== ================ =========== =========== ================ =======================
Repository     get              add         delete      update           replace
============== ================ =========== =========== ================ =======================
Python memory  list[...]        list.append list.pop    setattr          list.pop & list.append
SQL            SELECT           INSERT      DELETE      UPDATE           DELETE & INSERT
MongoDB        findOne          insertOne   deleteOne   updateOne        deleteOne & insertOne
REST (HTTP)    GET              POST        DELETE      PATCH            PUT
============== ================ =========== =========== ================ =======================

In addition, operations can also be divided to those that affect only one item and 
to those that affect multiple items. If you search for a car with registration 
number 123-456-789 you should should only get one car or none whereas if you 
search for cars with a red color you may get multiple, one or none. The same applies
for other operations as well.

Example
-------

First we create a simple in-memory repository that has ``registration_number`` as the attribute
that is unique for each item:

.. code-block::

  from redbird.repos import MemoryRepo
  repo = MemoryRepo(id_field="registration_number")

Create some items to the database:

.. code-block::

  repo.add({"registration_number": "123-456-789", "color": "red"})
  repo.add({"registration_number": "111-222-333", "color": "red"})
  repo.add({"registration_number": "444-555-666", "color": "blue"})

Get items from the database:

.. code-block::

  # One item
  repo["123-456-789"]

  # Multiple items
  repo.filter_by(color="red").all()

Update items in the database:

.. code-block::

  # One item
  repo["123-456-789"] = {"condition": "good"}

  # Multiple items
  repo.filter_by(color="blue").update(color="green")

Delete items from the database:

.. code-block::

  # One item
  del repo["123-456-789"]

  # Multiple items
  repo.filter_by(color="red").delete()


.. toctree::
   :maxdepth: 2
   :caption: Contents:

   getting_started
   repos/index
   crud
   operations
   logging_handler
   examples/index
   references/index
   versions

Indices and tables
==================

* :ref:`genindex`
