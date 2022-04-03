
from .base import BaseRepo, BaseResult
from . import _version
__version__ = _version.get_versions()['version']

def create_repo(model=None, model_orm=None, repo_name="MemoryRepo"):
    registry[repo_name](model=model, model_orm=model_orm)
