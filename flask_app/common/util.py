"""Utilities for repeated tasks."""
import os


# .....................................................................................
def get_host_url():
    """Get the URL for the host machine.

    Returns:
        a URL string for the host machine.
    """
    protocol = "https://"
    host_url = os.getenv("FQDN")
    # default
    if host_url is None:
        host_url = f"{protocol}localhost"
    # Use SSL
    if not host_url.startswith(protocol):
        host_url = f"{protocol}{host_url}"
    # Remove trailing slash
    if host_url.endswith("/"):
        host_url = host_url[:-1]
    return host_url
