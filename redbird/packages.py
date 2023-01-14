from typing import TYPE_CHECKING
from redbird.utils.importing import import_optional, import_exists

if TYPE_CHECKING:
    import sqlalchemy as sqlalchemy_lib
    import pymongo as pymongo_lib
    import requests as requests_lib
    import pydantic_sqlalchemy as pydantic_sqlalchemy_lib

sqlalchemy: 'sqlalchemy_lib' = import_optional("sqlalchemy")
pymongo: 'pymongo_lib' = import_optional("pymongo")
requests: 'requests_lib' = import_optional("requests")
pydantic_sqlalchemy: 'pydantic_sqlalchemy_lib' = import_optional("pydantic_sqlalchemy")