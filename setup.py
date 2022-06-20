from setuptools import setup, find_packages
import versioneer

with open("README.md", "r") as f:
    long_description = f.read()

setup(
    name="redbird",
    version=versioneer.get_version(),
    cmdclass=versioneer.get_cmdclass(),
    author="Mikael Koli",
    author_email="koli.mikael@gmail.com",
    url="https://github.com/Miksus/red-bird.git",
    packages=find_packages(),
    description="Repository Patterns for Python",
    long_description=long_description,
    long_description_content_type="text/markdown",
    classifiers=[
        "Operating System :: OS Independent",
        "License :: OSI Approved :: MIT License",
        "Development Status :: 4 - Beta",

        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9',
        'Programming Language :: Python :: 3.10',

        "Intended Audience :: Developers",

        'Topic :: Database',
    ],
    include_package_data=True, # for MANIFEST.in
    python_requires='>=3.7.0',
    extras_require={
        "full": ["sqlalchemy", "pymongo", "requests", "pydantic-sqlalchemy"],
        "full-test": [
            "sqlalchemy", "pymongo", "requests", "pydantic-sqlalchemy",
            "pytest", "python-dotenv", "responses", "mongomock"
        ],
        "sql": ["sqlalchemy", "pydantic-sqlalchemy"],
        "mongodb": ['pymongo'],
        "rest": ["requests"],
    },
    install_requires = ["pydantic", "typing_extensions"],
)
