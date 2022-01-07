"""Module to query GBIF APIs and return data."""
import os
import requests

from bison.common.constants import BISON_DELIMITER, ENCODING, GBIF
from bison.common.util import get_csv_writer

NEWLINE = "\n"
CR_RETURN = "\r"


# .............................................................................
class GbifAPI(object):
    """Pulls UUIDs and metadata for local resolution of GBIF Organizations, Providers, Resources."""

    # ...............................................
    def __init__(self):
        pass

    # ...............................................
    def _saveNLDelCR(self, strval):
        fval = strval.replace(NEWLINE, "\\n").replace("\r", "")
        return fval

    # ...............................................
    def _parse_date(self, datestr):
        datevals = []
        dateonly = datestr.split("T")[0]
        if dateonly != "":
            parts = dateonly.split("-")
            try:
                for i in range(len(parts)):
                    datevals.append(int(parts[i]))
            except ValueError:
                print("Invalid date {}".format(datestr))
                pass
            else:
                if len(datevals) not in (1, 3):
                    print("Non one or three part date {}".format(datevals))
        return datevals

    # ...............................................
    def _first_newer(self, datevals1, datevals2):
        for i in (0, 1, 2):
            # only compare matching year/mo/day
            if len(datevals1) > i and len(datevals2) > i:
                if datevals1[i] > datevals2[i]:
                    return True
                elif datevals1[i] < datevals2[i]:
                    return False
        # if equal, return first_newer
        return True

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
    def _get_buried_url_val(self, data_dict, search_prefix=None):
        url = ""
        try:
            idents = data_dict["identifiers"]
        except KeyError:
            pass
        else:
            for child in idents:
                if child["type"] == "URL":
                    url = child["identifier"]
                    # Return URL with desired prefix, else return last instance
                    if search_prefix and url.startswith(search_prefix):
                        break
        return url

    # ...............................................
    def _get_val(self, data_dict, keylist, save_nl=True):
        val = ""
        child = data_dict
        for key in keylist:
            try:
                child = child[key]
            except KeyError:
                pass
                # print("Key {} of {} missing from output".format(key, keylist))
            else:
                val = child
        if val != "" and save_nl:
            val = self._saveNLDelCR(val)
        return val

    # ...............................................
    def query_for_dataset(self, dataset_key):
        """Query the GBIF dataset API for a single record.

        Args:
            dataset_key (str): GBIF dataset UUID for query.

        Returns:
            dictionary of dataset metadata
        """
        dataset_meta = {}
        if dataset_key == "":
            return dataset_meta
        else:
            metaurl = GBIF.DATASET_URL + dataset_key
        data = self._get_data_from_url(metaurl)
        if data is None:
            return dataset_meta

        key = self._get_val(data, ["key"], save_nl=False)
        url = ""
        if key == "":
            print("Failed to resolve dataset with no key")
        else:
            orgkey = self._get_val(data, ["publishingOrganizationKey"], save_nl=True)
            if orgkey == "":
                print("Failed to find publishingOrganizationKey in {}".format(metaurl))
            if url == "":
                url = self._get_buried_url_val(data)
            if url == "":
                url = self._get_val_or_first_of_list(data, "homepage")

            title = self._get_val(data, ["title"], save_nl=True)
            desc = self._get_val(data, ["description"], save_nl=True)
            citation = self._get_val(data, ["citation", "text"], save_nl=True)
            created = self._get_val(data, ["created"], save_nl=False)
            modified = self._get_val(data, ["modified"], save_nl=False)

            # Matches current_gbif content of MERGED_RESOURCE_LUT_FIELDS
            dataset_meta["gbif_datasetkey"] = key
            dataset_meta["gbif_publishingOrganizationKey"] = orgkey
            dataset_meta["gbif_title"] = title
            dataset_meta["gbif_url"] = url
            dataset_meta["gbif_description"] = desc
            dataset_meta["gbif_citation"] = citation
            dataset_meta["gbif_created"] = created
            dataset_meta["gbif_modified"] = modified
        return dataset_meta

    # ...............................................
    def find_orguuid_from_dataset(self, dataset_key):
        """Query the GBIF dataset API with a UUID and return the owning organization UUID.

        Args:
            dataset_key (str): UUID for a dataset

        Returns:
            UUID for the dataset"s owning organization
        """
        dataset_meta = self.query_for_dataset(dataset_key)
        try:
            publishingOrgUUID = dataset_meta["publishingOrganizationKey"]
        except KeyError:
            print("No record for datasetKey {}".format(dataset_key))
        return publishingOrgUUID

    # ...............................................
    def _process_record(self, rec, header, reformat_keys=None):
        row = []
        if rec is not None:
            for key in header:
                try:
                    val = rec[key]

                    if type(val) is list:
                        if len(val) > 0:
                            val = val[0]
                        else:
                            val = ""

                    if reformat_keys and key in reformat_keys:
                        val = self._saveNLDelCR(val)

                    elif key == "citation":
                        if type(val) is dict:
                            try:
                                val = val["text"]
                            except KeyError:
                                pass

                    elif key in ("created", "modified"):
                        val = self._parse_date(val)

                except KeyError:
                    val = ""
                row.append(val)
        return row

    # ...............................................
    def query_write_all_meta(
        self, apitype, outfname, header, reformat_keys, delimiter=BISON_DELIMITER):
        """Query GBIF api for all records from a single service and write record outputs to a CSV file.

        This method pages the results by including the `offset` and `limit` parameters
        to get the records in chunks.

        Args:
            apitype (str): Type of GBIF API service to query
            outfname (str): Full filename for the output file
            header (list): Header for the output file
            reformat_keys (list): List of fieldnames for which to reformat the string EOL characters
            delimiter (str): Single character for output field delimiter

        Raises:
            Exception on failure to write
        """
        if apitype not in ("organization"):
            raise Exception("What kind of query is {}?".format(apitype))

        offset = 0
        pagesize = 1000
        url = "{}/{}?offset={}&limit={}".format(GBIF.URL, apitype, offset, pagesize)

        total = 0
        data = self._get_data_from_url(url)
        if data is not None:
            pcount = data["count"]
            allObjs = data["results"]
            isComplete = data["endOfRecords"]
            total = len(allObjs)
        if total == 0:
            print("No records returned for url {}".format(url))
            return
        with open(outfname, "w", encoding=ENCODING) as outf:
            outf.write(delimiter.join(header) + NEWLINE)

            recno = 0
            while total <= pcount:
                print("Received {} of {} {}s from GBIF".format(len(allObjs), pcount, apitype))
                for obj in allObjs:
                    recno += 1
                    row = self._process_record(obj, header, reformat_keys=reformat_keys)
                    try:
                        outf.write(delimiter.join(row) + NEWLINE)
                    except Exception:
                        raise

                if isComplete:
                    total = pcount + 1
                else:
                    offset += pagesize
                    url = "{}/{}?offset={}&limit={}".format(
                        GBIF.URL, apitype, offset, pagesize
                    )
                    data = self._get_data_from_url(url)
                    if data is not None:
                        allObjs = data["results"]
                        isComplete = data["endOfRecords"]
                        total += len(allObjs)

    # ...............................................
    def query_write_some_meta(
        self, apitype, outfname, header, reformat_keys, UUIDs, delimiter=BISON_DELIMITER):
        """Query GBIF api with a list of UUIDs and write record outputs to a CSV file.

        Args:
            apitype (str): Type of GBIF API service to query
            outfname (str): Full filename for the output file
            header (list): Header for the output file
            reformat_keys (list): List of fieldnames for which to reformat the string EOL characters
            UUIDs (list): List of UUIDs for query
            delimiter (str): Single character for output field delimiter

        Raises:
            Exception on failure to write
        """


        if apitype not in ("species", "dataset", "organization"):
            raise Exception("What kind of query is {}?".format(apitype))
        if not UUIDs:
            print("No UUIDs provided for GBIF {} query".format(apitype))
            return
        count = 0
        loginterval = len(UUIDs) // 10
        with open(outfname, "w", encoding=ENCODING) as outf:
            outf.write(delimiter.join(header) + NEWLINE)
            for uuid in UUIDs:
                count += 1
                url = "{}/{}/{}".format(GBIF.URL, apitype, uuid)
                data = self._get_data_from_url(url)
                row = self._process_record(data, header, reformat_keys=reformat_keys)
                if (count % loginterval) == 0:
                    print(
                        "*** Processed {} of {} {} queries ***".format(
                            count, len(UUIDs), apitype
                        )
                    )
                try:
                    outf.write(delimiter.join(row) + NEWLINE)
                except Exception:
                    print("Failed to write row {}".format(row))

    # ...............................................
    def get_write_all_organization_meta(self, outfname, delimiter=BISON_DELIMITER):
        """Query GBIF api for all organizations and write record outputs to a CSV file.

        Args:
            outfname (str): Full filename for the output file
            delimiter (str): Single character for output field delimiter
        """

        self.query_write_all_meta(
            GBIF.ORG_KEYS.apitype,
            outfname,
            GBIF.ORG_KEYS.saveme,
            GBIF.ORG_KEYS.preserve_format,
            delimiter=delimiter,
        )

    # ...............................................
    def get_write_dataset_meta(self, outfname, uuids, delimiter=BISON_DELIMITER):
        """Query GBIF api with a list of dataset UUIDs and write record outputs to a CSV file.

        Args:
            outfname (str): Full filename for the output file
            uuids (list): List of UUIDs for query
            delimiter (str): Single character for output field delimiter
        """
        self.query_write_some_meta(
            GBIF.DSET_KEYS.apitype,
            outfname,
            GBIF.DSET_KEYS.saveme,
            GBIF.DSET_KEYS.preserve_format,
            uuids,
            delimiter=delimiter,
        )

    # ...............................................
    def get_write_org_meta(self, outfname, uuids, delimiter=BISON_DELIMITER):
        """Query GBIF api with a list of organization UUIDs and write record outputs to a CSV file.

        Args:
            outfname (str): Full filename for the output file
            uuids (list): List of UUIDs for query
            delimiter (str): Single character for output field delimiter
        """
        self.query_write_some_meta(
            GBIF.ORG_KEYS.apitype,
            outfname,
            GBIF.ORG_KEYS.saveme,
            GBIF.ORG_KEYS.preserve_format,
            uuids,
            delimiter=delimiter,
        )

    # ...............................................
    def _get_val_or_first_of_list(self, data, key):
        val = ""
        if key in data:
            val = data[key]
            if isinstance(val, list) or isinstance(val, tuple):
                if len(val) > 0:
                    val = val[0]
                else:
                    val = ""
        return val


    # ...............................................
    def query_for_name(self, taxkey=None, sciname=None, kingdom=None):
        """Query the GBIF species service for taxonomic name elements.

        Args:
            taxkey (str): GBIF unique identifier for a taxonomic record
            sciname (str): Scientific name for a scientific record

        Returns:
            a dictionary of name elements

        Raises:
            Exception on failure provide either taxkey or sciname.
        """
        canonical = None

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
    def _post_json_to_parser(self, url, data):
        response = output = None
        try:
            response = requests.post(url, json=data)
        except Exception as e:
            if response is not None:
                retcode = response.status_code
            else:
                print("Failed on URL {} ({})".format(url, str(e)))
        else:
            if response.ok:
                try:
                    output = response.json()
                except Exception:
                    try:
                        output = response.content
                    except Exception:
                        output = response.text
                    # else:
                    #     print("Failed to interpret output of URL {} ({})".format(url, str(e)))
            else:

                try:
                    retcode = response.status_code
                    reason = response.reason
                except AttributeError:
                    print("Failed to find failure reason for URL {}".format(url))
                else:
                    print("Failed on URL {} ({}: {})".format(url, retcode, reason))
        return output

    # ...............................................
    def get_write_parsednames(self, indata, outfname, delimiter=BISON_DELIMITER):
        """Query the GBIF parser with a list of scientific names, write outputs to file.

        Write a CSV file containing records of scientific_name, canonical_name retrieved
        from the GBIF parser.

        Args:
            indata (str): string containing a comma-delimited list of names
            outfname (str): full path of the output file to write
            delimiter (str): single character to use to as a field delimiter in output file

        Returns:
            total (int): total number of records to write
            name_fail (list): list of scientific names that were not parsed
        """
        name_fail = []
        if os.path.exists(outfname):
            fmode = "a"
        else:
            fmode = "w"
        csvwriter, f = get_csv_writer(outfname, delimiter, ENCODING, fmode=fmode)

        output = self._post_json_to_parser(GBIF.BATCH_PARSER_URL, indata)
        total = 0

        if output is not None:
            total = len(output)
            try:
                for rec in output:
                    try:
                        sciname = rec["scientificName"]
                    except KeyError:
                        print("Missing scientificName in output record")
                    except Exception as e:
                        print(
                            "Failed reading scientificName in output record, err: {}".format(
                                e
                            )
                        )
                    else:
                        if rec["parsed"] is True:
                            try:
                                canname = rec["canonicalName"]
                                csvwriter.writerow([sciname, canname])
                            except KeyError:
                                print("Missing canonicalName in output record")
                            except Exception as e:
                                print("Failed writing output record, err: {}".format(e))
                        else:
                            name_fail.append(sciname)

            except Exception as e:
                print("Failed writing outfile {}, err: {}".format(outfname, e))
            finally:
                f.close()

        return total, name_fail

    # ...............................................
    def get_parsednames(self, indata):
        """Query the GBIF parser with a list of scientific names.

        Args:
            indata (str): string containing a comma-delimited list of names.

        Returns:
            parsed_names (dict): dictionary of scientific_name keys and canonical_name values.
            name_fail (list): list of scientific names that were not parsed.
        """
        name_fail = []
        parsed_names = {}
        output = self._post_json_to_parser(GBIF.BATCH_PARSER_URL, indata)
        if output is not None:
            for rec in output:
                try:
                    sciname = rec["scientificName"]
                except KeyError:
                    print("Missing scientificName in output record")
                except Exception as e:
                    print(
                        "Failed reading scientificName in output record, err: {}".format(
                            e
                        )
                    )
                else:
                    if rec["parsed"] is True:
                        try:
                            canname = rec["canonicalName"]
                            parsed_names[sciname] = canname
                        except KeyError as e:
                            print("Missing canonicalName in output record")
                        except Exception as e:
                            print("Failed writing output record, err: {}".format(e))
                    else:
                        name_fail.append(sciname)
        return parsed_names, name_fail


# ...............................................
if __name__ == "__main__":
    sciname = "Urochloa plantaginea (Link) R.D.Webster"
    gbifapi = GbifAPI()

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
