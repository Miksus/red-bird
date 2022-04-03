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
    url="https://github.com/Miksus/red-base.git",
    packages=find_packages(),
    description="Repository Patterns for Python",
    long_description=long_description,
    long_description_content_type="text/markdown",
    classifiers=[
        "Operating System :: OS Independent",
        "License :: OSI Approved :: MIT License",
        "Development Status :: 3 - Alpha",

        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.8",

        "Intended Audience :: Developers",
     ],
     include_package_data=True, # for MANIFEST.in
     python_requires='>=3.6.0',

    install_requires = [],
)
