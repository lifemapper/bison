"""Setup script for Glue Spark script using additional python libraries."""
from setuptools import setup

setup(
    name="pylibmodule",
    version="0.1",
    packages=[],
    install_requires=["sqlalchemy==2.0.23", "psycopg2>=2.9.6"]
)
