"""Functions for retrieving Specify Network webpages."""
# -*- coding: utf-8 -*-
import json
import os
from string import Template

base_dir = os.path.dirname(os.path.realpath(__file__))
templates_dir = os.path.join(base_dir, "templates/")

templates = dict()
is_development = True


# .............................................................................
def template(name, arguments):
    """Fill and return the HTML template with the given name and arguments.

    Args:
        name: template name
        arguments: dictionary of key/values to fill the template

    Returns:
        html webpage for the given template and data
    """
    if name not in templates or is_development:
        with open(os.path.join(templates_dir, f"{name}.html")) as file:
            templates[name] = Template(file.read()).substitute
    return templates[name](
        {
            key: "".join(value) if type(value) == list else value
            for key, value in arguments.items()
        }
    )


# .............................................................................
def inline_static(file_path):
    """Return the contents of a static file.

    Args:
        file_path: path to a file

    Returns:
        contents of a file
    """
    signature = (
        f"/* {file_path} */\n\n"
        if file_path.endswith("css") or file_path.endswith("js")
        else ""
    )
    with open(
        os.path.join(base_dir, file_path), "r", encoding="utf-8"
    ) as file:
        return f"{signature}{file.read()}"


# .............................................................................
def get_bundle_location(name):
    """Return the relative path to the location of a static js file.

    Args:
        name: name key of a filename value

    Returns:
        relative path to a file
    """
    manifest = json.loads(inline_static("/volumes/webpack-output/manifest.json"))
    file_name = os.path.basename(manifest[name])
    return f"/static/js/{file_name}"


# .............................................................................
def frontend_template():
    """Return the frontend index webpage.

    Returns:
        html webpage for the frontend index webpage.
    """
    return template(
        "index",
        {
            "bundle": get_bundle_location("frontend.js"),
            "description": (
                "This page compares the information contained in a specimen "
                "record in a Specify database with that held by biodiversity "
                "aggregators and name authorities. The page includes responses "
                "from global providers on the occurrence, taxonomy and "
                "geographic distribution of the species."
            ),
        },
    )


# .............................................................................
def stats_template():
    """Return the stats webpage.

    Returns:
        html webpage for the stats webpage.
    """
    return template(
        "index",
        {
            "bundle": get_bundle_location("stats.js"),
            "description": (
                "The maps on this page visualize the geographic locality of "
                "the digitized biological specimens held in museums and "
                "herbaria as reported to GBIF. The first map shows all "
                "vouchered species occurrence points by biological discipline "
                "within an institution. The second map visualizes the point "
                "localities for all digitized specimens, for all species, "
                "across all disciplines within an  institution."
            ),
        },
    )
