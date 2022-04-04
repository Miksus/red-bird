
# Red Bird: Repository Patterns for Python
> Generic database implemetation for SQL, MongoDB and in-memory lists

---

[![Pypi version](https://badgen.net/pypi/v/redbird)](https://pypi.org/project/redbird/)
[![build](https://github.com/Miksus/red-bird/actions/workflows/main.yml/badge.svg?branch=master)](https://github.com/Miksus/red-bird/actions/workflows/main.yml)
[![Documentation Status](https://readthedocs.org/projects/red-bird/badge/?version=latest)](https://red-bird.readthedocs.io)
[![PyPI pyversions](https://badgen.net/pypi/python/redbird)](https://pypi.org/project/redbird/)

Repository Pattern is a technique in which database layer is separated
from the application code. In other words, repository pattern provide
generic ways to communicate with the database that are genericnot database 
specific or even data store specific. Ultimately all data stores are just
for retrieving, updating, setting and deleting data.

In practice, repository pattern is an abstraction that provide same syntax 
regardless of the data store is SQL database, MongoDB database or even
just a Python list in RAM. 

Pros in repository pattern:
- More readable code
    - Every database connection follow the same pattern.
- More maintainable code
    - Database migrations are easy.
    - Unit testing requires no separate database for testing.
- More rapid development
    - Use Python lists until you get your database set up.

Cons in repository pattern:
- Poor at optimization
- Hides the actual operations


## Examples

First, we create a simple repo:

```python
from redbird.repos import MemoryRepo
repo = MemoryRepo()
```

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

## Creating Repository

In-memory repository:

```python
from redbird.repos import MemoryRepo
repo = MemoryRepo()
```

SQL repository:

```python
from sqlalchemy import create_engine
from redbird.repos import SQLRepo
repo = SQLRepo(table="mytable", engine=create_engine("sqlite://"))
```

or using ORM:

```python
from sqlalchemy.orm import declarative_base
from sqlalchemy import Column, String, Integer

Base = declarative_base()
class SQLModel(SQLBase):
    __tablename__ = 'pytest'
    id = Column(String, primary_key=True)
    name = Column(String)
    age = Column(Integer)

repo = SQLRepo(model_orm=SQLModel, engine=create_engine("sqlite://"))
```

## Author

* **Mikael Koli** - [Miksus](https://github.com/Miksus) - koli.mikael@gmail.com