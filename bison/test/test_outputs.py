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

        self._datapath, _ = os.path.split(csv_filename)
        self._csvfile = csv_filename

        in_base_filename, ext = os.path.splitext(csv_filename)
        base_pattern = f"{in_base_filename}"
        if do_split is True:
            base_pattern = f"{in_base_filename}_chunk"
        # Pattern for filenames with annotated records
        self.annotated_filename_pattern = os.path.join(self._datapath, f"{base_pattern}*annotated{ext}")
        # Pattern for filenames with summary of annotated records
        self.summary_annotated_pattern = os.path.join(self._datapath, f"{base_pattern}*annotated_summary{ext}")

        if logger is None:
            logger = get_logger(self._datapath)
        self._log = logger

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
    def _count_annotated_records_for_species(self, spname, taxkey, state, county):
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
    def _count_annotated_records_for_assessments(self, state, county):
        annotated_filenames = glob.glob(self.annotated_filename_pattern)
        state_occ_assessment_counts = RIIS_Counts(is_group=False, logger=self._log)
        cty_occ_assessment_counts = RIIS_Counts(is_group=False, logger=self._log)
        # Track species for each assessment in county and state
        cty_species = {}
        state_species = {}
        for ass in ASSESS_VALUES:
            cty_species[ass] = set()
            state_species[ass] = set()

        for fn in annotated_filenames:
            try:
                dwcdata = DwcData(fn)
                dwcdata.open()

                # Find the species of the first record with riis_assessment
                rec = dwcdata.get_record()
                while rec is not None:
                    assess = rec[NEW_RIIS_ASSESSMENT_FLD]
                    taxkey = rec[GBIF.ACC_TAXON_FLD]
                    if rec[NEW_RESOLVED_STATE] == state:
                        # Add to occ count
                        state_occ_assessment_counts.add_to(assess, value=1)
                        # Add to set of species
                        state_species[assess].add(taxkey)
                        if county is not None and rec[NEW_RESOLVED_COUNTY] == county:
                            # Add to occ count
                            cty_occ_assessment_counts.add_to(assess, value=1)
                            # Add to set of species
                            cty_species[assess].add(taxkey)

                    rec = dwcdata.get_record()
            except Exception as e:
                raise Exception(f"Unknown exception {e} on file {fn}")
            finally:
                dwcdata.close()
                dwcdata = None

        state_species_counts = RIIS_Counts(
            introduced=len(state_species["introduced"]), invasive=len(state_species["invasive"]),
            presumed_native=len(state_species["presumed_native"]), is_group=True, logger=self._log)
        cty_species_counts = RIIS_Counts(
            introduced=len(cty_species["introduced"]), invasive=len(cty_species["invasive"]),
            presumed_native=len(cty_species["presumed_native"]), is_group=True, logger=self._log)

        self._log.info(f"Counted species for assessments in {state} and {county} in {len(annotated_filenames)} annotated files")

        return state_occ_assessment_counts, cty_occ_assessment_counts, state_species_counts, cty_species_counts

    # .............................................................................
    def _get_assess_summary(self, state, county=None, is_group=False):
        species_counts = occ_counts = None
        assess_summary_fname = Aggregator.construct_assessment_summary_name(self._datapath)
        try:
            rdr, inf = get_csv_dict_reader(assess_summary_fname, GBIF.DWCA_DELIMITER)
            for rec in rdr:
                if rec[STATE_KEY] == state:
                    # occurrence counts
                    intro_occ = rec[INTRODUCED_SPECIES]
                    inv_occ = rec[INVASIVE_SPECIES]
                    native_occ = rec[NATIVE_SPECIES]
                    # species/group counts
                    intro_sp = rec[INTRODUCED_SPECIES]
                    inv_sp = rec[INVASIVE_SPECIES]
                    native_sp = rec[NATIVE_SPECIES]
                    if county is None:
                        if not rec[COUNTY_KEY]:
                            species_counts = RIIS_Counts(
                                introduced=intro_sp, invasive=inv_sp, presumed_native=native_sp, is_group=True,
                                logger=self._log)
                            occ_counts = RIIS_Counts(
                                introduced=intro_occ, invasive=inv_occ, presumed_native=native_occ, is_group=False,
                                logger=self._log)
                            break
                    elif rec[COUNTY_KEY] == county:
                        species_counts = RIIS_Counts(
                            introduced=intro_sp, invasive=inv_sp, presumed_native=native_sp, is_group=True,
                            logger=self._log)
                        occ_counts = RIIS_Counts(
                            introduced=intro_occ, invasive=inv_occ, presumed_native=native_occ, is_group=False,
                            logger=self._log)
                        break
        except Exception as e:
            raise Exception(f"Unexpected error {e} in opening or reading {assess_summary_fname}")
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
    def _log_comparison(self, truth_counts, summary_counts, compare_type, source):
        self._log.info(f"{compare_type} introduced, invasive, presumed_native count from annotated records: ")
        self._log.info(
            f"    {truth_counts.invasive}, {truth_counts.introduced}, {truth_counts.presumed_native}")
        self._log.info(f"{compare_type} introduced, invasive, presumed_native records from {source}: ")
        self._log.info(
            f"    {summary_counts.invasive}, {summary_counts.introduced}, {summary_counts.presumed_native}")
        if truth_counts.equals(summary_counts):
            self._log.info("Success!")
        else:
            self._log.info("Fail! Annotations do not match summaries")
        self._log.info("")

    # .............................................................................
    def compare_counts(self):
        """Compare the number of matching annotated records against the values in summary files."""
        # Get an invasive species occurrence from one annotated file
        filenames = glob.glob(self.annotated_filename_pattern)
        midx = int(len(filenames) / 2)
        species_name, taxkey, county, state = self._get_random_species(filenames[midx], "invasive")

        self._log.info("--------------------------------------")
        self._log.info(f"Compare `ground truth` occurrences of {species_name} in {county} {state} to region summaries")
        self._log.info("--------------------------------------")

        # Ground truth: Count matching lines for species and region in annotated records
        gtruth_state_occ_counts, gtruth_county_occ_counts = self._count_annotated_records_for_species(
            species_name, taxkey, state, county)

        # Counts from state and county summary files
        state_loc_occ_counts = self._get_region_count(species_name, state, county=None)
        county_loc_occ_counts = self._get_region_count(species_name, state, county=county)

        # Compare
        self._log_comparison(gtruth_state_occ_counts, state_loc_occ_counts, f"{state} {species_name} occurrence", f"{state} summary")
        self._log_comparison(gtruth_county_occ_counts, county_loc_occ_counts, f"{county} {species_name} occurrence", f"{county} summary")

        self._log.info("--------------------------------------")
        self._log.info(f"Compare `ground truth` assessment occurrences {county} {state} to RIIS summary")
        self._log.info("--------------------------------------")

        # Ground truth: Count matching lines for assessment and region in annotated records
        (gtruth_state_occ_riis_counts, gtruth_cty_occ_riis_counts, gtruth_state_species_counts,
            gtruth_cty_species_counts) = self._count_annotated_records_for_assessments(state, county)

        # Counts from RIIS assessment summary
        (st_species_counts, st_occ_counts) = self._get_assess_summary(state)
        (cty_species_counts, cty_occ_counts) = self._get_assess_summary(state, county=county)

        self._log_comparison(gtruth_state_occ_riis_counts, st_occ_counts, f"{state} occurrence", "RIIS summary")
        self._log_comparison(gtruth_cty_occ_riis_counts, cty_occ_counts, f"{county} occurrence", "RIIS summary")

        # Compare species counts
        self._log.info("--------------------------------------")
        self._log.info(f"Compare `ground truth` assessment species {county} {state} to RIIS summary")
        self._log.info("--------------------------------------")
        self._log_comparison(gtruth_state_species_counts, st_species_counts, f"{state} species", "RIIS summary")
        self._log_comparison(gtruth_cty_species_counts, cty_species_counts, f"{county} species", "RIIS summary")


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
