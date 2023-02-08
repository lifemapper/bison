"""Main script to execute all elements of the summarize-GBIF BISON workflow."""
import glob
import os

from bison.common.constants import (
    APPEND_TO_DWC, BIG_DATA_PATH, GBIF, LMBISON, LOG)
from bison.common.log import Logger
from bison.common.util import get_csv_dict_reader
from bison.process.aggregate import Aggregator, RIIS_Counts
from bison.providers.gbif_data import DwcData


# .............................................................................
class Counter():
    """Class for comparing counts for a RIIS assessment."""
    def __init__(self, csv_filename, logger, do_split=True):
        """Constructor.

        Args:
            csv_filename (str): full filename of input
            do_split (bool): True to test subsetted/chunked files; False to process
                csv_filename directly
            logger (object): logger for saving relevant processing messages

        Raises:
            FileNotFoundError: on missing input file.
        """
        if not os.path.exists(csv_filename):
            raise FileNotFoundError(f"File {csv_filename} does not exist")

        self._csvfile = csv_filename
        self._log = logger
        self._datapath, _ = os.path.split(csv_filename)

        in_base_filename, ext = os.path.splitext(csv_filename)
        base_pattern = f"{in_base_filename}"
        if do_split is True:
            base_pattern = f"{in_base_filename}_chunk"
        # Pattern for filenames with annotated records
        self.annotated_filename_pattern = os.path.join(
            self._datapath, f"{base_pattern}*annotated{ext}")
        # Pattern for filenames with summary of annotated records
        self.summary_annotated_pattern = os.path.join(
            self._datapath, f"{base_pattern}*annotated_summary{ext}")

    # .............................................................................
    def _get_random_species(self, annotated_occ_filename, assessment=ASSESS_VALUES[1]):
        # Get one species name, county, state with riis_assessment from annotated file
        accepted_spname = None
        dwcdata = DwcData(annotated_occ_filename, self._log)
        try:
            dwcdata.open()

            # Find the species of the first record with riis_assessment
            rec = dwcdata.get_record()
            while rec is not None:
                if rec[APPEND_TO_DWC.RIIS_ASSESSMENT] == assessment:
                    accepted_spname = rec[GBIF.ACC_NAME_FLD]
                    taxkey = rec[GBIF.ACC_TAXON_FLD]
                    county = rec[APPEND_TO_DWC.RESOLVED_CTY]
                    state = rec[APPEND_TO_DWC.RESOLVED_ST]
                    print(f"Found {accepted_spname} on line {dwcdata.recno}")
                    break
                rec = dwcdata.get_record()
        except Exception as e:
            raise Exception(f"Unknown exception {e} on file {annotated_occ_filename}")
        finally:
            dwcdata.close()

        if accepted_spname is None:
            raise Exception(f"No {assessment} records in {annotated_occ_filename}")

        return accepted_spname, taxkey, county, state

    # .............................................................................
    @classmethod
    def count_assessments(cls, annotated_occ_filename, logger):
        """Count records for each of the valid assessments in a file.

        Args:
            annotated_occ_filename (str): full filename of annotated file to summarize.
            logger (object): logger for saving relevant processing messages

        Returns:
            assessments (dict): dictionary with keys for each valid assessment type,
                and total record count for each.

        Raises:
            Exception: on unknown open or read error.
        """
        # Get one species name and county-state with riis_assessment from annotated
        # occurrences file.  Filtered records are retained, but have assessment = ""
        assessments = {"": 0}
        for val in ASSESS_VALUES:
            assessments[val] = 0

        dwcdata = DwcData(annotated_occ_filename, logger)
        try:
            dwcdata.open()

            # Find the species of the first record with riis_assessment
            rec = dwcdata.get_record()
            while rec is not None:
                ass = rec[APPEND_TO_DWC.RIIS_ASSESSMENT]
                try:
                    assessments[ass] += 1
                except Exception as e:
                    print(f"Here is e {e}")
                rec = dwcdata.get_record()
        except Exception as e:
            raise Exception(f"Unknown exception {e} on file {annotated_occ_filename}")
        finally:
            dwcdata.close()

        return assessments

    # .............................................................................
    def _count_annotated_records_for_species(self, spname, taxkey, state, county):
        annotated_filenames = glob.glob(self.annotated_filename_pattern)
        state_counts = RIIS_Counts(is_group=False, logger=self._log)
        cty_counts = RIIS_Counts(is_group=False, logger=self._log)
        for fn in annotated_filenames:
            try:
                dwcdata = DwcData(fn, self._log)
                dwcdata.open()

                # Find the species of the first record with riis_assessment
                rec = dwcdata.get_record()
                while rec is not None:
                    if (rec[GBIF.ACC_TAXON_FLD] == taxkey
                            and rec[APPEND_TO_DWC.RESOLVED_STATE] == state):
                        assess = rec[APPEND_TO_DWC.RIIS_ASSESSMENT_FLD]
                        # Add to state count
                        state_counts.add_to(assess, value=1)
                        if rec[APPEND_TO_DWC.RESOLVED_COUNTY] == county:
                            # Add to county count
                            cty_counts.add_to(assess, value=1)
                    rec = dwcdata.get_record()
            except Exception as e:
                raise Exception(f"Unknown exception {e} on file {fn}")
            finally:
                dwcdata.close()
                dwcdata = None

        self._log.log(
            f"Counted occurrences of {spname} in {state} and {county} in "
            f"{len(annotated_filenames)} annotated files",
            refname=self.__class__.__name__)

        return state_counts, cty_counts

    # .............................................................................
    def _count_annotated_records_for_assessments(self, state, county):
        annotated_filenames = glob.glob(self.annotated_filename_pattern)
        state_occ_assessment_counts = RIIS_Counts(self._log, is_group=False)
        cty_occ_assessment_counts = RIIS_Counts(self._log, is_group=False)
        # Track species for each assessment in county and state
        cty_species = {}
        state_species = {}
        for ass in ASSESS_VALUES:
            cty_species[ass] = set()
            state_species[ass] = set()

        for fn in annotated_filenames:
            try:
                dwcdata = DwcData(fn, self._log)
                dwcdata.open()

                # Find the species of the first record with riis_assessment
                rec = dwcdata.get_record()
                while rec is not None:
                    assess = rec[APPEND_TO_DWC.RIIS_ASSESSMENT_FLD]
                    taxkey = rec[GBIF.ACC_TAXON_FLD]
                    if rec[APPEND_TO_DWC.RESOLVED_STATE] == state:
                        # Add to occ count
                        state_occ_assessment_counts.add_to(assess, value=1)
                        # Add to set of species
                        state_species[assess].add(taxkey)
                        if county is not None and rec[APPEND_TO_DWC.RESOLVED_COUNTY] == county:
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
            self._log, introduced=len(state_species["introduced"]),
            invasive=len(state_species["invasive"]),
            presumed_native=len(state_species["presumed_native"]),
            is_group=True)
        cty_species_counts = RIIS_Counts(
            self._log, introduced=len(cty_species["introduced"]),
            invasive=len(cty_species["invasive"]),
            presumed_native=len(cty_species["presumed_native"]),
            is_group=True)

        self._log.log(
            f"Counted species for assessments in {state} and {county} in "
            f"{len(annotated_filenames)} annotated files", refname=self.__class__.__name__)

        return state_occ_assessment_counts, cty_occ_assessment_counts, state_species_counts, cty_species_counts

    # .............................................................................
    def _get_assess_summary(self, state, county=None, is_group=False):
        species_counts = occ_counts = None
        assess_summary_fname = Aggregator.construct_assessment_summary_name(self._datapath)
        try:
            rdr, inf = get_csv_dict_reader(assess_summary_fname, GBIF.DWCA_DELIMITER)
            for rec in rdr:
                if rec[LMBISON.STATE_KEY] == state:
                    # occurrence counts
                    intro_occ = rec[LMBISON.INTRODUCED_OCCS]
                    inv_occ = rec[LMBISON.INVASIVE_OCCS]
                    native_occ = rec[LMBISON.NATIVE_OCCS]
                    # species/group counts
                    intro_sp = rec[LMBISON.INTRODUCED_SPECIES]
                    inv_sp = rec[LMBISON.INVASIVE_SPECIES]
                    native_sp = rec[LMBISON.NATIVE_SPECIES]
                    if county is None:
                        if not rec[LMBISON.COUNTY_KEY]:
                            species_counts = RIIS_Counts(
                                self._log, introduced=intro_sp, invasive=inv_sp,
                                presumed_native=native_sp, is_group=True)
                            occ_counts = RIIS_Counts(
                                self._log, introduced=intro_occ, invasive=inv_occ,
                                presumed_native=native_occ, is_group=False)
                            break
                    elif rec[LMBISON.COUNTY_KEY] == county:
                        species_counts = RIIS_Counts(
                            self._log, introduced=intro_sp, invasive=inv_sp,
                            presumed_native=native_sp, is_group=True)
                        occ_counts = RIIS_Counts(
                            self._log, introduced=intro_occ, invasive=inv_occ,
                            presumed_native=native_occ, is_group=False)
                        break
        except Exception as e:
            raise Exception(
                f"Unexpected error {e} in opening or reading {assess_summary_fname}")
        return (species_counts, occ_counts)

    # .............................................................................
    def _get_region_count(self, acc_species_name, state, county=None):
        region_summary_fname = Aggregator.construct_location_summary_name(
            self._datapath, state, county=county)
        # Get counts for all assessments of species_key in this region summary file
        loc_occ_counts = RIIS_Counts(self._log, is_group=False)
        try:
            rdr, inf = get_csv_dict_reader(region_summary_fname, GBIF.DWCA_DELIMITER)
        except Exception as e:
            raise Exception(f"Unexpected open error {e} in {region_summary_fname}")

        try:
            for rec in rdr:
                if rec[GBIF.ACC_NAME_FLD] == acc_species_name:
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
            raise Exception(f"Unexpected read error {e} in {region_summary_fname}")
        finally:
            inf.close()

        self._log.log(
            f"Read occurrence summary for {acc_species_name} in "
            f"{region_summary_fname} summary file", refname=self.__class__.__name__)

        return loc_occ_counts

    # .............................................................................
    def _log_comparison(self, truth_counts, summary_counts, compare_type, source):
        self._log.log(
            f"Compare annotation counts to {source} ({compare_type}) introduced, "
            f"invasive, presumed_native: ", refname=self.__class__.__name__)
        self._log.log(
            f"    {truth_counts.introduced}, {truth_counts.invasive}, "
            f"{truth_counts.presumed_native}", refname=self.__class__.__name__)
        self._log.log(
            f"    {summary_counts.introduced}, {summary_counts.invasive}, "
            f"{summary_counts.presumed_native}", refname=self.__class__.__name__)
        if truth_counts.equals(summary_counts):
            self._log.log("Success!", refname=self.__class__.__name__)
        else:
            self._log.log(
                "FAIL! Annotations do not match summaries",
                refname=self.__class__.__name__)
        self._log.log("", refname=self.__class__.__name__)

    # .............................................................................
    def compare_counts(self):
        """Compare matching annotated records against the counts in summary files."""
        # Get an invasive species occurrence from one annotated file
        filenames = glob.glob(self.annotated_filename_pattern)
        midx = int(len(filenames) / 2)
        acc_species_name, taxkey, county, state = self._get_random_species(
            filenames[midx], "invasive")

        self._log.log(
            "--------------------------------------", refname=self.__class__.__name__)
        self._log.log(
            f"Compare `ground truth` occurrences of '{acc_species_name}' in "
            f"{county} {state} to region summaries", refname=self.__class__.__name__)
        self._log.log(
            "--------------------------------------", refname=self.__class__.__name__)

        # Ground truth: Count matching lines for species and region in annotated records
        gtruth_state_occXspecies, gtruth_county_occXspecies = \
            self._count_annotated_records_for_species(
                acc_species_name, taxkey, state, county)

        # Counts from state and county summary files
        state_loc_occ_counts = self._get_region_count(
            acc_species_name, state, county=None)
        county_loc_occ_counts = self._get_region_count(
            acc_species_name, state, county=county)

        # Compare
        self._log_comparison(
            gtruth_state_occXspecies, state_loc_occ_counts,
            f"{state} {acc_species_name} occurrence", f"{state} summary")
        self._log_comparison(
            gtruth_county_occXspecies, county_loc_occ_counts,
            f"{county} {acc_species_name} occurrence", f"{county} summary")

        self._log.log(
            "--------------------------------------", refname=self.__class__.__name__)
        self._log.log(
            f"Compare `ground truth` assessment (occurrences) {county} {state} to " +
            "RIIS summary", refname=self.__class__.__name__)
        self._log.log(
            "--------------------------------------", refname=self.__class__.__name__)

        # Ground truth: Count matching lines for assessment and region in annotated recs
        (gtruth_state_occXassess, gtruth_cty_occXassess, gtruth_state_speciesXassess,
            gtruth_cty_speciesXassess) = self._count_annotated_records_for_assessments(
            state, county)

        # Counts from RIIS assessment summary
        (st_species_counts, st_occ_counts) = self._get_assess_summary(state)
        (cty_species_counts, cty_occ_counts) = self._get_assess_summary(
            state, county=county)

        # Compare
        self._log_comparison(
            gtruth_state_occXassess, st_occ_counts,
            f"{state} occurrence", "RIIS summary")
        self._log_comparison(
            gtruth_cty_occXassess, cty_occ_counts,
            f"{county} occurrence", "RIIS summary")

        # Compare species counts
        self._log.log(
            "--------------------------------------", refname=self.__class__.__name__)
        self._log.log(
            f"Compare `ground truth` assessment (species) {county} {state} to RIIS "
            f"summary", refname=self.__class__.__name__)
        self._log.log(
            "--------------------------------------", refname=self.__class__.__name__)
        self._log_comparison(
            gtruth_state_speciesXassess, st_species_counts,
            f"{state} species", "RIIS summary")
        self._log_comparison(
            gtruth_cty_speciesXassess, cty_species_counts,
            f"{county} species", "RIIS summary")


# .............................................................................
# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    import argparse

    gbif_infile = os.path.join(BIG_DATA_PATH, "gbif_2022-03-16.csv")

    parser = argparse.ArgumentParser(
        description="Test outputs of summarized GBIF data with RIIS assessments.")
    parser.add_argument(
        "big_csv_filename", type=str, default=gbif_infile,
        help="The full path to GBIF input species occurrence data.")
    parser.add_argument(
        "--do-split", type=str, choices=("True", "False"), default="True",
        help="True to test subsetted/chunked files; False to process file directly.")

    args = parser.parse_args()
    base_big_csv_filename = args.big_csv_filename
    do_split = True if args.do_split.lower() in ("yes", "y", "true", "1") else False

    # ...............................................
    # Test data
    # ...............................................
    base_big_csv_filename = "gbif_2022-03-16_100k.csv"
    script_name = os.path.splitext(os.path.basename(__file__))[0]
    # ...............................................

    # # Basename for constructing temporary files
    in_base_filename, ext = os.path.splitext(base_big_csv_filename)
    # Full path to input data
    big_csv_filename = os.path.join(BIG_DATA_PATH, base_big_csv_filename)
    logger = Logger(
        script_name, os.path.join(BIG_DATA_PATH, LOG.DIR, f"{script_name}".log))

    record_counter = Counter(big_csv_filename, logger, do_split=True)
    record_counter.compare_counts()
