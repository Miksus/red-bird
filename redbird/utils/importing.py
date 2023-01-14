import importlib

def get_import_error(name:str):
    raise ModuleNotFoundError(f"No module named '{name}'")

class _Missing_Package:
    def __init__(self, name:str) -> None:
        self.__name__ = name

    def __setattr__(self, name, value):
        if name == "__name__":
            super().__setattr__(name, value)
            return
        raise get_import_error(self.__name__)

    def __getattr__(self, name):
        raise get_import_error(self.__name__)
    

def import_optional(name:str):
    try:
        return importlib.import_module(name)
    except ModuleNotFoundError:
        return _Missing_Package(name)

def import_exists(name:str):
    return not isinstance(import_optional(name), _Missing_Package)