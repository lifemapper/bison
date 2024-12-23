"""Class for the Specify Network Name API service."""
from werkzeug.exceptions import BadRequest

from flask_app.common.constants import APIService, BisonOutput
from flask_app.bison.base import _BisonService

from bison.common.util import (
    add_errinfo, combine_errinfo, get_traceback, prettify_object)


# .............................................................................
class DescribeSvc(_BisonService):
    """Specify Network API service for information about aggregated occurrence data."""
    SERVICE_TYPE = APIService.Describe
    ORDERED_FIELDNAMES = []

    # ...............................................
    @classmethod
    def get_measures(cls, summary_type=None, summary_key=None):
        """Return descriptive measurements for one or all dataset/species.

        Args:
            summary_type: data dimension for summary, ("species" or "dataset")
            summary_key: unique identifier for the data dimension being examined.  If
                None, return stats for all identifiers.

        Returns:
            full_output (flask_app.common.s2n_type.AnalystOutput): including a
                dictionary (JSON) of a record containing keywords with values.
        """
        if summary_type is None and summary_key is None:
            return cls.get_endpoint()

        stat_dict = {}
        try:
            good_params, errinfo = cls._standardize_params(
                summary_type=summary_type, summary_key=summary_key)
        except BadRequest as e:
            errinfo = {"error": [e.description]}
        except Exception:
            errinfo = {"error": [get_traceback()]}

        else:
            if good_params["summary_type"] is not None:
                try:
                    stat_dict, errors = cls._get_measures(
                        good_params["summary_type"], good_params["summary_key"])
                except Exception:
                    errinfo = add_errinfo(errinfo, "error", get_traceback())
                else:
                    errinfo = combine_errinfo(errinfo, errors)
            else:
                options = cls.SERVICE_TYPE["params"]["summary_type"]["options"]
                errinfo = {
                    "error": [f"Must provide summary_type key with value in {options}"]}

        # Assemble
        full_out = BisonOutput(
            cls.SERVICE_TYPE["name"], description=cls.SERVICE_TYPE["description"],
            output=stat_dict, errors=errinfo)

        return full_out.response

    # ...............................................
    @classmethod
    def _get_measures(cls, summary_type, summary_key):
        stat_dict = {}
        errinfo = {"error": ["The BISON `describe` service is not yet implemented"]}
        out_dict = {f"{summary_type.capitalize()} Statistics":  stat_dict}
        return out_dict, errinfo


# .............................................................................
if __name__ == "__main__":
    summary_type = "state"
    summary_key = "KS"
    svc = DescribeSvc()

    print("**** Endpoint ****")
    response = svc.get_endpoint()
    print(prettify_object(response))

    print("**** Describe ****")
    response = svc.get_measures(summary_type=summary_type, summary_key=summary_key)
    print(prettify_object(response))

"""
from flask_app.analyst.describe import *

dataset_key = "3e2d26d9-2776-4bec-bdc7-bab3842ffb6b"
key_species = "11378306 Phaneroptera laticerca"

svc = DescribeSvc()
response = svc.get_endpoint()
print(prettify_object(response))

response = svc.get_measures(summary_type="dataset", summary_key=dataset_key)
print(prettify_object(response))

response = svc.get_measures(summary_type="species", summary_key=key_species)
print(prettify_object(response))

response = svc.get_measures(summary_type="dataset")
print(prettify_object(response))

response = svc.get_measures(summary_type="species")
print(prettify_object(response))
"""
