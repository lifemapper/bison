"""Module to test the contents of the input GBIF csv occurrence data file."""
import os
from bison.common.constants import GBIF, PARAMETERS
from bison.common.log import Logger
from bison.tools._config_parser import process_arguments_from_file
from bison.provider.gbif_data import DwcData

logger = Logger(os.path.splitext(os.path.basename(__file__))[0])
config_filename = "/volumes/bison/tests/config/test_process_gbif.json"
params = process_arguments_from_file(config_filename, PARAMETERS)


# .............................................................................
class TestGBIFData():
    """Class for testing downloaded simple GBIF CSV file."""

    # .....................................
    def test_read_header(self):
        """Test reading the header of a GBIF CSV file.

        Raises:
            Exception: on failure to open or read record from DwcData object.
        """
        dwc = DwcData(params["gbif_filename"], logger)
        try:
            dwc.open()
            fldnames = dwc.fieldnames
            for fn in GBIF.REQUIRED_FIELDS():
                assert(fn in fldnames)
        except Exception as e:
            raise Exception(f"Failed to open or read DwcData {params['gbif_filename']}: {e}")
        finally:
            dwc.close()

    #
    # .....................................
    def test_read_rec(self):
        """Test reading a record from a GBIF CSV file.

        Raises:
            Exception: on failure to open or read record from DwcData object.
        """
        dwc = DwcData(params["gbif_filename"], logger)
        try:
            dwc.open()
            rec = dwc.get_record()
        except Exception as e:
            raise Exception(f"Failed to open or read DwcData {params['gbif_filename']}: {e}")
        else:
            assert(isinstance(rec, dict))
        finally:
            dwc.close()

    # .....................................
    def test_read_all_recs(self):
        """Test reading all records from a GBIF CSV file.

        Raises:
            Exception: on failure to open DwcData object.
            Exception: on failure to read record from DwcData object.
        """
        dwc = DwcData(params["gbif_filename"], logger)
        count = 0
        try:
            dwc.open()
            rec = dwc.get_record()
            while rec is not None:
                count += 1
                try:
                    rec = dwc.get_record()
                except Exception as e:
                    raise Exception(f"Failed to read record at {dwc.recno}: {e}")
        except Exception as e:
            raise Exception(f"Failed to open DwcData {params['gbif_filename']}: {e}")
        else:
            assert(count == params["_test_gbif_record_count"])
        finally:
            dwc.close()

    # # ...............................................
    # def test_gbif_name_accepted(self):
    #     """Open a GBIF datafile with a csv.DictReader."""
    #     svc = GbifSvc()
    #     self.open()
    #     for rec in self._gbif_reader:
    #         if rec is None:
    #             break
    #         elif (self.recno % 100) == 0:
    #             self.logit('*** Record number {} ***'.format(self.recno))
    #         taxkey = rec[GBIF.TAXON_FLD]
    #         # API data
    #         taxdata = svc.query_for_name(taxkey=taxkey)
    #         taxstatus = taxdata[GBIF.STATUS_FLD]
    #         taxname = taxdata[GBIF.NAME_FLD]
    #         # Make sure simple CSV data taxonkey is accepted
    #         if taxstatus.lower() != "accepted":
    #             self.logit("Record {} taxon key {} is not an accepted name {}".format(
    #                 rec[GBIF.ID_FLD], taxkey, taxstatus))
    #         # Make sure simple CSV data sciname matches name for taxonkey
    #         if rec[GBIF.NAME_FLD] != taxname:
    #             self.logit("Record {} name {} does not match taxon key {} name {}".format(
    #                 rec[GBIF.ID_FLD], rec[GBIF.NAME_FLD], taxkey, taxname))
