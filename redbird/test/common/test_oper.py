
from redbird.oper import skip, Between, LessEqual, between, GreaterEqual


def test_between_open_left():
    oper = between(None, 30, none_as_open=True)
    assert isinstance(oper, LessEqual)
    assert oper.value == 30

def test_between_open_right():
    oper = between(30, None, none_as_open=True)
    assert isinstance(oper, GreaterEqual)
    assert oper.value == 30

def test_between_open_both():
    oper = between(None, None, none_as_open=True)
    assert oper is skip

def test_between_strict_none():
    oper = between(None, None)
    assert isinstance(oper, Between)
    assert oper.start is None
    assert oper.end is None