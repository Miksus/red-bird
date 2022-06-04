![Red Bird](https://raw.githubusercontent.com/Miksus/red-bird/master/docs/logo.svg)


# Repository Patterns for Python
> Generic database implemetation for SQL, MongoDB and in-memory lists

---

[![Pypi version](https://badgen.net/pypi/v/redbird)](https://pypi.org/project/redbird/)
[![build](https://github.com/Miksus/red-bird/actions/workflows/main.yml/badge.svg?branch=master)](https://github.com/Miksus/red-bird/actions/workflows/main.yml)
[![codecov](https://codecov.io/gh/Miksus/red-bird/branch/master/graph/badge.svg?token=GVBWCKHL1N)](https://codecov.io/gh/Miksus/red-bird)
[![Documentation Status](https://readthedocs.org/projects/red-bird/badge/?version=latest)](https://red-bird.readthedocs.io)
[![PyPI pyversions](https://badgen.net/pypi/python/redbird)](https://pypi.org/project/redbird/)

Repository pattern is a technique to abstract the data access from
the domain/business logic. In other words, it decouples the database 
access from the application code. The aim is that the code runs the 
same regardless if the data is stored to an SQL database, NoSQL 
database, file or even as an in-memory list.

Read more about the repository patterns in the [official documentation](https://red-bird.readthedocs.io).

## Why?

Repository pattern has several benefits over embedding the 
database access to the application:

- Faster prototyping and development
- Easier to migrate the database
- More readable code, Pythonic
- Unit testing and testing in general is safer and easier 

## Features

Main features:

- Support for Pydantic models for data validation
- Identical way to create, read, update and delete (CRUD)
- Pythonic and simple syntax
- Support for more complex queries (greater than, not equal, less than etc.)

Supported repositories:

- SQL
- MongoDB
- In-memory (Python list)
- JSON files
- CSV file

## Examples

First, we create a simple repo:

```python
from redbird.repos import MemoryRepo
repo = MemoryRepo()
```

> Note: the following examples work on any repository, not just in-memory repository.

Adding/creating items:

```python
repo.add({"name": "Anna", "nationality": "British"})
```

Reading items:

```python
repo.filter_by(name="Anna").all()
```

Updating items:

```python
repo.filter_by(name="Anna").update(nationality="Finnish")
```

Deleting items:

```python
repo.filter_by(name="Anna").delete()
```

See more from the [official documentation](https://red-bird.readthedocs.io).

## Author

* **Mikael Koli** - [Miksus](https://github.com/Miksus) - koli.mikael@gmail.com