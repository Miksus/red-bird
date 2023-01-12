import pytest
from redbird.utils.importing import import_optional

def test_import():
    pkg: pytest = import_optional("pytest")
    assert pkg is pytest
    assert pkg.main is pytest.main

def test_import_fail():
    pkg = import_optional("redbird_missing_package")
    assert pkg.__name__ == "redbird_missing_package"
    with pytest.raises(ModuleNotFoundError):
        pkg.session
    with pytest.raises(ModuleNotFoundError):
        pkg.session = "value"
