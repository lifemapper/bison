"""Tools to be called from a Docker container in an EC2 instance."""
from . import annotate_riis

__all__ = [
    "annotate_riis",
]

__version__ = "2.0"
