"""Main script to execute all elements of the summarize-GBIF BISON workflow."""
import glob
import os

from bison.common.aggregate import Aggregator, RIIS_Counts
from bison.common.gbif import DwcData
from bison.common.constants import (
    ASSESS_VALUES, DATA_PATH, GBIF, NEW_RESOLVED_COUNTY, INTRODUCED_SPECIES, INVASIVE_SPECIES, NATIVE_SPECIES,
    NEW_RESOLVED_STATE, NEW_RIIS_ASSESSMENT_FLD, SPECIES_KEY, STATE_KEY, COUNTY_KEY, ASSESS_KEY, COUNT_KEY)
from bison.tools.util import get_csv_dict_reader, get_logger


# .............................................................................
class Counter():
    """Class for comparing counts for a RIIS assessment."""
    def __init__(self, csv_filename, do_split=True, logger=None):
        """Constructor.

        Args:
            csv_filename (str): full filename of input
            do_split (bool): True to test subsetted/chunked files; False to process csv_filename directly
            logger (object): logger for saving relevant processing messages

        Raises:
            FileNotFoundError: on missing input file.
        """
        if not os.path.exists(csv_filename):
            raise FileNotFoundError(f"File {csv_filename} does not exist")

        # Basename for constructing temporary files
        in_base_filename, ext = os.path.splitext(csv_filename)
        base_pattern = f"{in_base_filename}"
        if do_split is True:
            base_pattern = f"{in_base_filename}_chunk"
        # Pattern for filenames with annotated records
        self.annotated_filename_pattern = os.path.join(self._datapath, f"{base_pattern}*annotated{ext}")
        # Pattern for filenames with summary of annotated records
        self.summary_annotated_pattern = os.path.join(self._datapath, f"{base_pattern}*annotated_summary{ext}")

        self._datapath, _ = os.path.split(csv_filename)
        self._csvfile = csv_filename
        if logger is None:
            logger = get_logger(self._datapath)
        self._log = logger

        # RIIS assessment summary file
        self.assess_summary_fname = Aggregator.construct_assessment_summary_name(self._datapath)

    # .............................................................................
    def _get_random_species(self, annotated_occ_filename, assessment=ASSESS_VALUES[1]):
        # Get one species name and county-state with riis_assessment from annotated occurrences file
        spname = None
        dwcdata = DwcData(annotated_occ_filename)
        try:
            dwcdata.open()

            # Find the species of the first record with riis_assessment
            rec = dwcdata.get_record()
            while rec is not None:
                if rec[NEW_RIIS_ASSESSMENT_FLD] == assessment:
                    spname = rec[GBIF.ACC_NAME_FLD]
                    taxkey = rec[GBIF.ACC_TAXON_FLD]
                    county = rec[NEW_RESOLVED_COUNTY]
                    state = rec[NEW_RESOLVED_STATE]
                    print(f"Found {spname} on line {dwcdata.recno}")
                    break
                rec = dwcdata.get_record()
        except Exception as e:
            raise Exception(f"Unknown exception {e} on file {annotated_occ_filename}")
        finally:
            dwcdata.close()

        if spname is None:
            raise Exception(f"No {assessment} records found in {annotated_occ_filename}")

        return spname, taxkey, county, state

    # .............................................................................
    def _count_annotated_records(self, spname, taxkey, state, county):
        annotated_filenames = glob.glob(self.annotated_filename_pattern)
        state_counts = RIIS_Counts(is_group=False, logger=self._log)
        cty_counts = RIIS_Counts(is_group=False, logger=self._log)
        for fn in annotated_filenames:
            try:
                dwcdata = DwcData(fn)
                dwcdata.open()

                # Find the species of the first record with riis_assessment
                rec = dwcdata.get_record()
                while rec is not None:
                    if rec[GBIF.ACC_TAXON_FLD] == taxkey and rec[NEW_RESOLVED_STATE] == state:
                        assess = rec[NEW_RIIS_ASSESSMENT_FLD]
                        # Add to state count
                        state_counts.add_to(assess, value=1)
                        if rec[NEW_RESOLVED_COUNTY] == county:
                            # Add to county count
                            cty_counts.add_to(assess, value=1)
                    rec = dwcdata.get_record()
            except Exception as e:
                raise Exception(f"Unknown exception {e} on file {fn}")
            finally:
                dwcdata.close()
                dwcdata = None

        self._log.info(f"Counted occurrences of {spname} in {state} and {county} in {len(annotated_filenames)} annotated files")

        return state_counts, cty_counts

    # .............................................................................
    def _get_assess_summary(self, state, county=None):
        species_counts = occ_counts = None
        try:
            rdr, inf = get_csv_dict_reader(self.assess_summary_fname, GBIF.DWCA_DELIMITER)
            for rec in rdr:
                if rec[STATE_KEY] == state:
                    intro_ct = rec[INTRODUCED_SPECIES]
                    inv_ct = rec[INVASIVE_SPECIES]
                    native_ct = rec[NATIVE_SPECIES]
                    if county is None:
                        if not rec[COUNTY_KEY]:
                            species_counts = RIIS_Counts(
                                introduced=intro_ct, invasive=inv_ct, presumed_native=native_ct, is_group=True,
                                logger=self._log)
                            occ_counts = RIIS_Counts(
                                introduced=intro_ct, invasive=inv_ct, presumed_native=native_ct, is_group=False,
                                logger=self._log)
                            break
                    elif rec[COUNTY_KEY] == county:
                        species_counts = RIIS_Counts(
                            introduced=intro_ct, invasive=inv_ct, presumed_native=native_ct, is_group=True,
                            logger=self._log)
                        occ_counts = RIIS_Counts(
                            introduced=intro_ct, invasive=inv_ct, presumed_native=native_ct, is_group=False,
                            logger=self._log)
                        break
        except Exception as e:
            raise Exception(f"Unexpected error {e} in opening or reading {self.assess_summary_fname}")
        return (species_counts, occ_counts)

    # .............................................................................
    def _get_region_count(self, species_name, state, county=None):
        region_summary_fname = Aggregator.construct_location_summary_name(self._datapath, state, county=county)
        # Get counts for all assessments of species_key in this region summary file
        loc_occ_counts = RIIS_Counts(is_group=False, logger=self._log)
        try:
            rdr, inf = get_csv_dict_reader(region_summary_fname, GBIF.DWCA_DELIMITER)
            for rec in rdr:
                if rec[SPECIES_KEY] == species_name:
                    rass = rec[ASSESS_KEY]
                    count = int(rec[COUNT_KEY])
                    if rass == "introduced":
                        loc_occ_counts.introduced = count
                    elif rass == "invasive":
                        loc_occ_counts.invasive = count
                    elif rass == "presumed_native":
                        loc_occ_counts.presumed_native = count
                    else:
                        raise Exception(f"Unknown record field {rass}.")
                    break
        except Exception as e:
            raise Exception(f"Unexpected open or read error {e} in {region_summary_fname}")
        finally:
            inf.close()

        self._log.info(f"Read occurrence summary for {species_name} in {region_summary_fname} summary file")

        return loc_occ_counts

    # .............................................................................
    def compare_counts(self):
        """Compare the number of matching annotated records against the values in summary files."""
        # Get an invasive species occurrence from one annotated file
        filenames = glob.glob(self.annotated_filename_pattern)
        midx = int(len(filenames) / 2)
        species_name, taxkey, county, state = self._get_random_species(filenames[midx], "invasive")

        # Ground truth: Count matching lines in annotated records
        state_ann_occ_counts, county_ann_occ_counts = self._count_annotated_records(
            species_name, taxkey, state, county)

        # Counts from state and county summary files
        state_loc_occ_counts = self._get_region_count(species_name, state, county=None)
        county_loc_occ_counts = self._get_region_count(species_name, state, county=county)

        # Counts from RIIS assessment summary
        (st_species_counts, st_occ_counts) = self._get_assess_summary(state)
        (cty_species_counts, cty_occ_counts) = self._get_assess_summary(state, county=county)

        # Count annotated records vs location summaries
        # County
        if county_ann_occ_counts.equals(county_loc_occ_counts):
            self._log.info("Success! Counts from annotated files match county location file summaries")
        else:
            self._log.info("Fail! Counts from annotated files match county location file summaries")
        # State
        if state_ann_occ_counts.equals(state_loc_occ_counts):
            self._log.info("Success! Counts from annotated files match state location file summaries")
        else:
            self._log.info("Fail! Counts from annotated files match state location file summaries")

        # Count annotated records vs assessment summary
        # County
        if county_ann_occ_counts.equals(cty_occ_counts):
            self._log.info("Success! Counts from annotated files match assessment summary")
        else:
            self._log.info("Fail! Counts from annotated files match assessment summary")
        # State
        if state_ann_occ_counts.equals(st_occ_counts):
            self._log.info("Success! Counts from annotated files match assessment summary")
        else:
            self._log.info("Fail! Counts from annotated files match assessment summary")


# .............................................................................
# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    import argparse

    gbif_infile = os.path.join(DATA_PATH, "gbif_2022-03-16.csv")

    parser = argparse.ArgumentParser(
        description="Test outputs of summarized GBIF data with RIIS assessments.")
    parser.add_argument(
        "big_csv_filename", type=str, default=gbif_infile,
        help="The full path to GBIF input species occurrence data.")
    parser.add_argument(
        "--do-split", type=str, choices=("True", "False"), default="True",
        help="True to test subsetted/chunked files; False to process big_csv_filename directly.")

    args = parser.parse_args()
    base_big_csv_filename = args.big_csv_filename
    do_split = True if args.do_split.lower() in ("yes", "y", "true", "1") else False
    logger = get_logger(DATA_PATH, logname="main_test_outputs")

    # ...............................................
    # Test data
    # ...............................................
    base_big_csv_filename = "gbif_2022-03-16_100k.csv"
    # ...............................................

    # # Basename for constructing temporary files
    in_base_filename, ext = os.path.splitext(base_big_csv_filename)
    # Full path to input data
    big_csv_filename = os.path.join(DATA_PATH, args.big_csv_filename)

    record_counter = Counter(big_csv_filename, do_split=True, logger=logger)
    record_counter.compare_counts()
