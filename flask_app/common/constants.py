"""Constants common to the Specify Network Broker and Analyst API services."""
import typing

URL_ESCAPES = [[" ", r"\%20"], [",", r"\%2C"]]
ENCODING = "utf-8"

# Used in broker, so relative to the flask_app/broker or analyst directories
STATIC_DIR = "../../bison/frontend/static"
ICON_DIR = f"{STATIC_DIR}/icon"
SCHEMA_DIR = f"{STATIC_DIR}/schema"

TEMPLATE_DIR = "../templates"
SCHEMA_FNAME = "open_api.yaml"


# .............................................................................
class BisonKey:
    """Keywords in a valid BISON API response."""
    # standard service output keys
    COUNT = "count"
    DESCRIPTION = "description"
    RECORD_FORMAT = "record_format"
    RECORDS = "records"
    OUTPUT = "output"
    ERRORS = "errors"
    # output one service at a time
    SERVICE = "service"
    # other keys
    NAME = "name"
    # input request multiple services
    SERVICES = "services"
    PARAM = "param"


# .............................................................................
class APIEndpoint:
    """URL elements for a valid BISON API request."""
    Root = "api/v1"
    Describe = "describe"

    @classmethod
    def get_endpoints(cls):
        """Get the endpoints for all BISON API services.

        Returns:
            list of all Endpoints
        """
        return [f"{cls.Root}/{svc}" for svc in [cls.Describe]]


# .............................................................................
class APIService:
    """Endpoint, parameters, output format for all Specify Network Broker APIs."""
    BaseBison = {
        "name": "",
        "endpoint": "",
        "params": {},
        "description": "",
        BisonKey.RECORD_FORMAT: None
    }
    BisonRoot = {
        "name": "root",
        "endpoint": APIEndpoint.Root,
        "params": {},
        "description": "",
        BisonKey.RECORD_FORMAT: None
    }
    Describe = {
        "name": "describe",
        "endpoint": APIEndpoint.Describe,
        "params": {
            "summary_type": {
                "type": "",
                "description":
                    "Type or dimension of data to describe (i.e: state, county, aiannh)",
                "options": ["state", "county", "aiannh"],
                "default": None
            },
            "summary_key": {
                "type": "",
                "description":
                    "Identifier of type of data to describe (i.e: KS, Douglas County_KS)",
                "default": None
            },

        },
        "description": "",
        BisonKey.RECORD_FORMAT: None
    }


# .............................................................................
class BisonOutput(object):
    """Response type for a BISON query."""
    service: str
    description: str = ""
    # records: typing.List[dict] = []
    records: typing.List = []
    errors: dict = {}

    # ...............................................
    def __init__(self, service, description=None, output=None, errors=None):
        """Constructor.

        Args:
            service: API Service this object is responding to.
            description: Description of the computation in this response.
            output: Statistics (dict) in this response.
            errors: Errors encountered when generating this response.
        """
        if errors is None:
            errors = {}
        if description is None:
            description = ""
        if output is None:
            output = {}
        # Dictionary is json-serializable
        self._response = {
            BisonKey.SERVICE: service,
            BisonKey.DESCRIPTION: description,
            BisonKey.OUTPUT: output,
            BisonKey.ERRORS: errors
        }

    # ...............................................
    @property
    def response(self):
        """Return the S2nOutput query response.

        Returns:
            the response object
        """
        return self._response

    # ....................................
    @classmethod
    def print_output(cls, response_dict, do_print_rec=False):
        """Print a formatted string of the elements in an S2nOutput query response.

        Args:
            response_dict: bison.common.constants.BisonOutput._response dictionary
            do_print_rec: True to print each record in the response.
        """
        print("*** Bison output ***")
        for name, attelt in response_dict.items():
            try:
                if name == "records" and do_print_rec:
                    print("records: ")
                    for rec in attelt:
                        print(rec)
                else:
                    print(f"{name}: {attelt}")
            except Exception:
                pass
