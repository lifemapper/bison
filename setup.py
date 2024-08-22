# -*- coding: utf-8 -*-
"""Setup module for bison."""
from setuptools import setup, find_packages


with open("README.md") as f:
    readme = f.read()

with open("LICENSE") as f:
    license = f.read()

setup(
    name="bison",
    version="2.0",
    description="Package of Bison objects and tools",
    long_description=readme,
    author="Specify Systems Team",
    author_email="aimee.stewart@ku.edu",
    url="https://github.com/lifemapper/bison",
    license=license,
    packages=find_packages(exclude=("obsolete", "tests", "docs")),
    install_requires=[
        "boto3",
        "fastparquet",
        "gdal",
        "numpy",
        "pandas",
        "requests",
        "rtree",
        "s3fs"
    ],
)
