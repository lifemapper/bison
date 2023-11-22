"""Setup script for Glue Spark script using additional python libraries."""
from setuptools import setup

setup(
    name="pylibmodule",
    version="0.1",
    packages=[],
    install_requires=["sqlalchemy==2.0.23", "pandas==0.25.3", "pymysql==0.9.3"]
)
