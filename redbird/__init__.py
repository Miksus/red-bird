
from .base import BaseRepo, BaseResult

try:
    from ._version import *
except ImportError:
    # Package was not built the standard way
    __version__ = version = '0.0.0.UNKNOWN'
    __version_tuple__ = version_tuple = (0, 0, 0, 'UNKNOWN', '')

def create_repo(model=None, orm=None, repo_name="MemoryRepo"):
    registry[repo_name](model=model, orm=orm)
