
# Red Bird: Repository Patterns for Python
> Generic database implemetation for SQL, MongoDB and in-memory lists

NOTE: Experimential.

## Examples

```python

class Person(BaseModel):
    id: str
    name: str
    age: int

```

### Creating

```python
>>> repo = Repository()

# Add some items
>>> repo.add(Person(id="11-11-11", name="Jack", age=30, language="English"))
>>> repo.add(Person(id="22-22-22", name="John", age=33, language="English"))
>>> repo.add(Person(id="33-33-33", name="James", age=36, language="English"))
>>> repo.add(Person(id="44-44-44", name="Jaakko", age=40, language="Finnish"))
```

### Reading

```python
# Get an item
>>> repo["11-11-11"]
Person(id="11-11-11", name="Jack", age=30, language="English")

>>> # Filter items
>>> repo.filter_by(language="English").all()
[Person(id="11-11-11", name="Jack", age=30, language="English"),
 Person(id="22-22-22", name="John", age=33, language="English"),
 Person(id="33-33-33", name="James", age=36, language="English")]

>>> # Get first
>>> repo.filter_by(language="English").first()
Person(id="11-11-11", name="Jack", age=30, language="English")

>>> # Get last
>>> repo.filter_by(language="English").last()
Person(id="33-33-33", name="John", age=33, language="English")

>>> # Get first 2
>>> repo.filter_by(language="English").limit(2)
[Person(id="11-11-11", name="Jack", age=30, language="English"),
 Person(id="22-22-22", name="John", age=33, language="English")]

>>> # Use operations
>>> from redbird.operations import greater_than
>>> repo.filter_by(age=greater_than(35)).all()
[Person(id="33-33-33", name="James", age=36, language="English"),
 Person(id="44-44-44", name="Jaakko", age=40, language="Finnish")]
```

### Updating
```python

>>> # Update single item
>>> person = repo["44-44-44"]
>>> person.age = 50
>>> repo.update(person)

>>> # Update multiple items
>>> repo.filter_by(language="English").update(age=35)
```

### Deleting
```python

>>> # Delete single item
>>> del repo["44-44-44"]

>>> # Delete multiple items
>>> repo.filter_by(language="English").delete()
```

## Author

* **Mikael Koli** - [Miksus](https://github.com/Miksus) - koli.mikael@gmail.com