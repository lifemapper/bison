"""Module to query GBIF APIs and return data."""
import requests

from bison.common.constants import GBIF

NEWLINE = "\n"
CR_RETURN = "\r"


# .............................................................................
class GbifSvc(APISvc):
    """Pulls UUIDs and metadata for local resolution of GBIF Organizations, Providers, Resources."""

    # # ...............................................
    # def __init__(self):
    #     """Construct GBIF API service."""
    #     pass
    #
    # # ...............................................
    # def _saveNL_delCR(self, strval):
    #     fval = strval.replace(NEWLINE, "\\n").replace("\r", "")
    #     return fval
    #
    # # ...............................................
    # def _parse_date(self, datestr):
    #     datevals = []
    #     dateonly = datestr.split("T")[0]
    #     if dateonly != "":
    #         parts = dateonly.split("-")
    #         try:
    #             for i in range(len(parts)):
    #                 datevals.append(int(parts[i]))
    #         except ValueError:
    #             print("Invalid date {}".format(datestr))
    #             pass
    #         else:
    #             if len(datevals) not in (1, 3):
    #                 print("Non one or three part date {}".format(datevals))
    #     return datevals
    #
    # # ...............................................
    # def _first_newer(self, datevals1, datevals2):
    #     for i in (0, 1, 2):
    #         # only compare matching year/mo/day
    #         if len(datevals1) > i and len(datevals2) > i:
    #             if datevals1[i] > datevals2[i]:
    #                 return True
    #             elif datevals1[i] < datevals2[i]:
    #                 return False
    #     # if equal, return first_newer
    #     return True

    # ...............................................
    def _get_data_from_url(self, url, resp_type="json"):
        data = None
        try:
            response = requests.get(url)
        except Exception:
            print("Failed to resolve URL {}".format(url))
        else:
            if response.status_code == 200:
                if resp_type == "json":
                    data = response.json()
                else:
                    data = response.text

            if data is not None:
                try:
                    result_count = data["count"]
                except KeyError:
                    pass
                else:
                    if result_count == 0:
                        data = None
        return data

    # ...............................................
    def query_for_dataset(self, dataset_key):
        """Query the GBIF dataset API for a single record.

        Args:
            dataset_key (str): GBIF dataset UUID for query.

        Returns:
            dictionary of GBIF dataset
        """
        data = self._get_data_from_url(GBIF.DATASET_URL + dataset_key)
        return data

    # ...............................................
    def find_orguuid_from_dataset(self, dataset_key):
        """Query the GBIF dataset API with a UUID and return the owning organization UUID.

        Args:
            dataset_key (str): UUID for a dataset

        Returns:
            UUID for the dataset"s owning organization
        """
        publishingOrgUUID = None
        dataset_rec = self.query_for_dataset(dataset_key)
        try:
            publishingOrgUUID = dataset_rec["publishingOrganizationKey"]
        except KeyError:
            print("No record for datasetKey {}".format(dataset_key))
        return publishingOrgUUID

    # ...............................................
    def query_for_name(self, taxkey=None, sciname=None, kingdom=None):
        """Query the GBIF species service for taxonomic name elements.

        Args:
            taxkey (str): GBIF unique identifier for a taxonomic record
            sciname (str): Scientific name for a scientific record
            kingdom (str): Kingdom for scientific name to search

        Returns:
            a dictionary of name elements

        Raises:
            Exception: on failure to provide either taxkey or sciname.
        """
        if taxkey is not None:
            url = "{}{}".format(GBIF.TAXON_URL(), taxkey)
            data = self._get_data_from_url(url)

        elif sciname is not None:
            # for replaceStr, withStr in GBIF.URL_ESCAPES:
            #     sciname = sciname.replace(replaceStr, withStr)
            url = "{}?name={}".format(GBIF.FUZZY_TAXON_URL(), sciname)
            if kingdom:
                url = "{}&kingdom={}".format(url, kingdom)
            data = self._get_data_from_url(url)
            if data is not None:
                if type(data) is list and len(data) > 0:
                    data = data[0]
        else:
            raise Exception("Must provide taxkey or sciname")

        return data


# ...............................................
if __name__ == "__main__":
    sciname = "Urochloa plantaginea (Link) R.D.Webster"
    gbifapi = GbifSvc()

    rec = gbifapi.query_for_name(sciname=sciname)
    print(rec)

"""
curl -i \
  --user usr:pswd \
  -H "Content-Type: application/json" \
  -H "Accept: application/json" \
  -X POST -d @/state/partition1/data/bison/us/nameUUIDForLookup_1-10000000_sciname.json \
  http://api.gbif.org/v1/parser/name


import csv
import codecs
import cStringIO
import json
import requests

from src.gbif.constants import *
from src.gbif.gbifresolve import *

gc = GBIFCodes()
header = ["legacyid", "key", "title", "description", "created",
             "modified", "homepage"]
offset = 0
pagesize = 1000
legacyid = 9999
desc = homepage = ""
url = "{}/organization?offset={}&limit={}".format(GBIF.URL, offset, pagesize)
response = requests.get(url)
data = json.load(response)
total = data["count"]
allProvs = data["results"]
isComplete = data["endOfRecords"]
"""
