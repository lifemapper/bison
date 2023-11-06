# -*- coding: utf-8 -*-
"""Setup module for lmbison."""
from setuptools import setup, find_packages


with open("README.md") as f:
    readme = f.read()

with open("LICENSE") as f:
    license = f.read()

setup(
    name="lmbison",
    version="0.5.0",
    description="Package of Lifemapper-Bison objects and tools",
    long_description=readme,
    author="Specify Systems Lifemapper Team",
    author_email="aimee.stewart@ku.edu",
    url="https://github.com/lifemapper/bison",
    license=license,
    packages=find_packages(exclude=("tests", "docs")),
    install_requires=[
        "gdal",
        "numpy",
        "pandas",
        "requests",
        "rtree",
    ],
)
