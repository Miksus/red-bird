
.. meta::
   :description: Red Bird is a repository pattern library for Python.
   :keywords: repository, pattern, Python, database

Red-Base: Repository Patterns for Python
========================================

Repository pattern is a technique to abstract the data access from
the domain/business logic. In other words, it decouples the database 
access from the application code. The aim is that the code runs the 
same regardless if the data is stored to an SQL database, NoSQL 
database, file or even as an in-memory list.

This has several benefits:

- Unit testing is easier
- Unit testing can be more thorough
- Database migrations require no code changes
- Using different databases adds no extra maintenance
- No need for copy databases in development

However, some features are database or data format specific which 
cannot be replicated in a way that works in all commonly used 
data storaging formats. Therefore, repository pattern cannot and 
is not meant to replace all plain SQL queries or other query
languages. However, most applications don't require optimized queries 
or complex communication.

Terminology
-----------

In Red Bird, the term *repository* is used to describe a 
data store and *item* is used to describe a record or 
a document. The definition of these terms between varios
data stores are illustrated below:

============== ===================== ============
Repository     Repository definition Item definition
============== ===================== ============
Python memory  list                  object
SQL            table                 row 
MongoDB        collection            document
REST (HTTP)    URL endpoint          JSON object
============== ===================== ============

In order to unify the access, common methods are used to do similar or the same
operations across data stores. These unified methods are illustrated below:

============== ================ =========== =========== ================ =======================
Repository     repo.get         repo.add    repo.delete repo.update      repo.replace
============== ================ =========== =========== ================ =======================
Python memory  list.__getitem__ list.append list.pop    setattr          repo.delete & repo.add
SQL            SELECT           INSERT      DELETE      UPDATE           repo.delete & repo.add
MongoDB        findOne          insertOne   deleteOne   updateOne        repo.delete & repo.add
REST (HTTP)    GET              POST        DELETE      PATCH            PUT
============== ================ =========== =========== ================ =======================



.. toctree::
   :maxdepth: 2
   :caption: Contents:

   getting_started
   repos/index
   crud
   filtering
   versions

Indices and tables
==================

* :ref:`genindex`
