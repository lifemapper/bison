"""Parent Class for the BISON API services."""
from flask import Flask
from logging import INFO
import os
from werkzeug.exceptions import BadRequest, InternalServerError

from bison.common.log import Logger
from bison.common.util import add_errinfo, get_today_str, get_traceback
from flask_app.common.constants import APIEndpoint, APIService, BisonOutput

try:
    # For docker deployment
    WORKING_PATH = os.environ["WORKING_DIRECTORY"]
    # Read-only volume
    AWS_INPUT_PATH = os.environ["AWS_DATA_DIRECTORY"]
except KeyError:
    # For local debugging
    WORKING_PATH = '/tmp'
    AWS_INPUT_PATH = WORKING_PATH
LOG_PATH = os.path.join(WORKING_PATH, "log")

app = Flask(__name__)


# .............................................................................
@app.errorhandler(BadRequest)
def handle_bad_request(e):
    """Return text explaining a bad request.

    Args:
        e: an Exception object

    Returns:
        string response.
    """
    return f"Bad request: {e}"


# .............................................................................
@app.errorhandler(InternalServerError)
def handle_bad_response(e):
    """Return text explaining a bad response.

    Args:
        e: an Exception object

    Returns:
        string response.
    """
    return f"Internal Server Error: {e}"


# .............................................................................
class _BisonService(object):
    """Base BISON service, handles parameter names and acceptable values."""
    # overridden by subclasses
    SERVICE_TYPE = APIService.BaseBison

    # .............................................................................
    @classmethod
    def _get_valid_requested_params(cls, user_params_string, valid_params):
        """Return valid and invalid options for parameters that accept >1 values.

        Args:
            user_params_string: user-requested parameters as a string.
            valid_params: valid parameter values

        Returns:
            valid_requested_params: list of valid params from the provided query string
            invalid_params: list of invalid params from the provided query string

        Note:
            For the badge service, exactly one provider is required.  For all other
            services, multiple providers are accepted, and None indicates to query all
            valid providers.
        """
        valid_requested_params = invalid_params = []

        if user_params_string:
            tmplst = user_params_string.split(",")
            user_params = {tp.lower().strip() for tp in tmplst}

            valid_requested_params = set()
            invalid_params = set()
            # valid_requested_providers, invalid_providers =
            #   cls.get_multivalue_options(user_provs, valid_providers)
            for param in user_params:
                if param in valid_params:
                    valid_requested_params.add(param)
                else:
                    invalid_params.add(param)

            invalid_params = list(invalid_params)
            if valid_requested_params:
                valid_requested_params = list(valid_requested_params)
            else:
                valid_requested_params = []

        return valid_requested_params, invalid_params

    # .............................................................................
    @classmethod
    def endpoint(cls):
        """Return the URL endpoint for this class.

        Returns:
            URL endpoint for the service
        """
        endpoint = f"{APIEndpoint.Root}/{cls.SERVICE_TYPE['endpoint']}"
        return endpoint

    # ...............................................
    @classmethod
    def _fix_type_new(cls, key, provided_val):
        """Modify a parameter value to a valid type and value.

        Args:
            key: parameter key
            provided_val: user-provided parameter value

        Returns:
            usr_val: a valid value for the parameter
            valid_options: list of valid options (for error message)

        Note:
            Corrections:
            * cast to correct type
            * validate with any options
            * if value is invalid (type or value), return the default.
        """
        valid_options = None
        if provided_val is None:
            return None
        # all strings are lower case
        try:
            provided_val = provided_val.lower()
        except Exception:
            pass

        param_meta = cls.SERVICE_TYPE["params"][key]
        # First see if restricted to options
        default_val = param_meta["default"]
        type_val = param_meta["type"]
        # If restricted options, check
        try:
            options = param_meta["options"]
        except KeyError:
            options = None
        else:
            # Invalid option returns default value
            if provided_val in options:
                usr_val = provided_val
            else:
                valid_options = options
                usr_val = default_val

        # If not restricted to options
        if options is None:
            # Cast values to correct type. Failed conversions return default value
            if isinstance(type_val, str) and not options:
                usr_val = str(provided_val)

            # Boolean also tests as int, so try boolean first
            elif isinstance(type_val, bool):
                if provided_val in (0, "0", "n", "no", "f", "false"):
                    usr_val = False
                elif provided_val in (1, "1", "y", "yes", "t", "true"):
                    usr_val = True
                else:
                    valid_options = (True, False)
                    usr_val = default_val
            else:
                usr_val = cls._test_numbers(provided_val, param_meta)

        return usr_val, valid_options

    # ...............................................
    @classmethod
    def _test_numbers(cls, provided_val, param_meta):
        default_val = param_meta["default"]
        type_val = param_meta["type"]
        # If restricted numeric values, check
        try:
            min_val = param_meta["min"]
        except KeyError:
            min_val = None
        # If restricted numeric values, check
        try:
            max_val = param_meta["max"]
        except KeyError:
            max_val = None

        if isinstance(type_val, float):
            try:
                usr_val = float(provided_val)
            except ValueError:
                usr_val = default_val
            else:
                if min_val and usr_val < min_val:
                    usr_val = min_val
                if max_val and usr_val > max_val:
                    usr_val = max_val

        elif isinstance(type_val, int):
            try:
                usr_val = int(provided_val)
            except ValueError:
                usr_val = default_val
            else:
                if min_val and usr_val < min_val:
                    usr_val = min_val
                if max_val and usr_val > max_val:
                    usr_val = max_val

        else:
            usr_val = provided_val

        return usr_val

    # ...............................................
    @classmethod
    def _process_params(cls, user_kwargs=None):
        """Modify all user provided keys to lowercase and values to correct types.

        Args:
            user_kwargs: dictionary of keywords and values sent by the user for
                the current service.

        Returns:
            good_params: dictionary of valid parameters and values
            errinfo: dictionary of errors for different error levels.

        Note:
            A list of valid values for a keyword can include None as a default
                if user-provided value is invalid
        Todo:
            Do we need not_in_valid_options for error message?
        """
        good_params = {}
        errinfo = {}

        # Correct all parameter keys/values present
        for key, param_meta in cls.SERVICE_TYPE["params"].items():
            val = user_kwargs[key]
            # Done in calling function
            if key == "provider":
                pass

            # Do not edit namestr, maintain capitalization
            elif key in ("namestr", "species_key", "summary_key"):
                good_params[key] = val

            # Require one valid icon_status
            elif key == "icon_status":
                valid_stat = param_meta["options"]
                if val is None:
                    errinfo = add_errinfo(
                        errinfo, "error",
                        f"Parameter {key} containing one of {valid_stat} options is "
                        f"required")
                elif val not in valid_stat:
                    errinfo = add_errinfo(
                        errinfo, "error",
                        f"Value {val} for parameter {key} not in valid options "
                        f"{valid_stat}")
                else:
                    good_params[key] = val

            elif val is not None:
                usr_val, valid_options = cls._fix_type_new(key, val)
                if valid_options is not None and val not in valid_options:
                    errinfo = add_errinfo(
                        errinfo, "error",
                        f"Value {val} for parameter {key} is not in valid options "
                        f"{param_meta['options']}")
                    good_params[key] = None
                else:
                    good_params[key] = usr_val

        # Fill in defaults for missing parameters
        for key in cls.SERVICE_TYPE["params"]:
            param_meta = cls.SERVICE_TYPE["params"][key]
            try:
                _ = good_params[key]
            except KeyError:
                good_params[key] = param_meta["default"]

        return good_params, errinfo

    # ..........................
    @staticmethod
    def OPTIONS():
        """Common options request for all services (needed for CORS)."""
        return

    # ...............................................
    @classmethod
    def get_endpoint(cls, **kwargs):
        """Return the http response for this class endpoint.

        Args:
            **kwargs: keyword arguments are accepted but ignored

        Returns:
            flask_app.analyst.s2n_type.S2nOutput object

        Raises:
            Exception: on unknown error.
        """
        try:
            output = cls._show_online()
        except Exception:
            raise
        return output.response

    # ...............................................
    @classmethod
    def _show_online(cls):
        svc = cls.SERVICE_TYPE["name"]
        info = {
            "info": "Bison {} service is online.".format(svc)}

        param_lst = []
        for p, pdict in cls.SERVICE_TYPE["params"].items():
            pinfo = pdict.copy()
            pinfo["type"] = str(type(pinfo["type"]))
            param_lst.append({p: pinfo})
        info["parameters"] = param_lst

        output = BisonOutput(
            svc, description=cls.SERVICE_TYPE["description"], errors=info)
        return output

    # ...............................................
    @classmethod
    def _init_logger(cls):
        script_name = os.path.splitext(os.path.basename(__file__))[0]
        todaystr = get_today_str()
        log_name = f"{script_name}_{todaystr}"
        logger = Logger(log_name, log_console=True, log_level=INFO)
        return logger

    # ...............................................
    @classmethod
    def _standardize_params(
            cls, summary_type=None, summary_key=None, rank_by=None, order=None,
            limit=10):
        """Standardize query parameters to send to appropriate service.

        Args:
            summary_type: data dimension for summary, comparisons, rank
            summary_key: unique identifier for the data dimension being examined.
            rank_by: rank by occurrence counts or measurement of another dimension of
                the data
            order: sort records "descending" or "ascending"
            limit: integer indicating how many ranked records to return, value must
                be less than QUERY_LIMIT.

        Raises:
            BadRequest: on invalid query parameters.
            BadRequest: on summary_type == rank_by for Rank service.
            BadRequest: on unknown exception parsing parameters.

        Returns:
            a dictionary containing keys and properly formatted values for the
                user specified parameters.
        """
        user_kwargs = {
            "summary_type": summary_type,
            "summary_key": summary_key,
            "rank_by": rank_by,
            "order": order,
            "limit": limit
        }
        try:
            usr_params, errinfo = cls._process_params(user_kwargs)
        except Exception:
            error_description = get_traceback()
            raise BadRequest(error_description)

        # In RankSvc, summary_type and rank_by may not be the same data dimension
        if rank_by is not None and usr_params["summary_type"] == usr_params["rank_by"]:
            raise BadRequest(
                f"Cannot rank by the same dimension as summarizing by. "
                f"URL arguments summary_type ({usr_params['summary_type']}) "
                f"and rank_by ({usr_params['rank_by']}) may not be equal.")

        # errinfo["error"] indicates bad parameters, throws exception
        try:
            error_description = "; ".join(errinfo["error"])
            raise BadRequest(error_description)
        except KeyError:
            pass

        return usr_params, errinfo

    # ...............................................
    @classmethod
    def _test_download(cls, filename):
        success = True
        msg = ""
        # Test successful download
        retries = 0
        interval = 6
        while not os.path.exists(filename) and retries < 10:
            import time
            time.sleep(6)
            retries += 1
        if not os.path.exists(filename):
            success = False
            msg = f"Failed to access {filename} in {retries * interval} seconds"
        return success, msg


# .............................................................................
if __name__ == "__main__":
    pass
