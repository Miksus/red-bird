

from abc import ABC
from typing import TYPE_CHECKING, Callable

if TYPE_CHECKING:
    from redbird.base import BaseResult

class Operation(ABC):
    """Field operation
    """
    __py_magic__: str
    __formatter__ : str
    def __init__(self, value):
        self.value = value

    def _get_formatter(self, result:'BaseResult') -> Callable:
        return getattr(result, self.__formatter__)

    def evaluate(self, value):
        return getattr(self, self.__py_magic__)(value)

class GreaterThan(Operation):
    __py_magic__ = "__gt__"
    __formatter__ = "format_greater_than"
    def __gt__(self, value):
        return value > self.value

class LessThan(Operation):
    __py_magic__ = "__lt__"
    __formatter__ = "format_less_than"
    def __lt__(self, value):
        return value < self.value

class GreaterEqual(Operation):
    __py_magic__ = "__ge__"
    __formatter__ = "format_greater_equal"
    def __ge__(self, value):
        return value >= self.value

class LessEqual(Operation):
    __py_magic__ = "__le__"
    __formatter__ = "format_less_equal"
    def __le__(self, value):
        return value <= self.value

class Equal(Operation):
    __py_magic__ = "__eq__"
    __formatter__ = "format_equal"
    def __eq__(self, value):
        return value == self.value

class NotEqual(Operation):
    __py_magic__ = "__ne__"
    __formatter__ = "format_not_equal"
    def __ne__(self, value):
        return value != self.value

class Between(Operation):

    def __init__(self, start, end):
        self.start = start
        self.end = end

    def evaluate(self, value):
        return self.start <= value <= self.end

class In(Operation):
    __py_magic__ = "__contains__"

    def evaluate(self, value):
        return value in self.value

class _Skip(Operation):
    """Field operator that does not affect filtering.
    This is just a convenient placeholder."""

    _instance = None

    def __new__(cls):
        if cls._instance is None:
            # Create the instance
            cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self):
        pass

    def evaluate(self, value):
        return True

def greater_than(value):
    return GreaterThan(value)
    
def less_than(value):
    return LessThan(value)

def greater_equal(value):
    return GreaterEqual(value)
    
def less_equal(value):
    return LessEqual(value)

def equal(value):
    return Equal(value)

def not_equal(value):
    return NotEqual(value)

def between(start, end, none_as_open=False):
    if none_as_open:
        # Handle Nones
        if start is None and end is None:
            return skip
        elif end is None:
            return greater_equal(start)
        elif start is None:
            return less_equal(end)
    return Between(start, end)

def in_(values):
    return In(values)

skip = _Skip()