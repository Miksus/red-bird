import sys

sys.stderr.write("""
Unsupported installation method: python setup.py
Please use `python -m pip install .` instead.
"""
)
#sys.exit(1)
from setuptools import setup

setup(
    name="redbird",
    install_requires = ["pydantic", "typing_extensions"],
)
