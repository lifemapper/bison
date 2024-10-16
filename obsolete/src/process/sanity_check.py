"""Common classes for adding USGS RIIS info to GBIF occurrences."""
import logging

from obsolete.src.common.constants2 import (
    APPEND_TO_DWC, GBIF, LMBISON, REGION)
from obsolete.src.common.util import get_csv_dict_reader
from bison.provider.gbif_data import DwcData


# .............................................................................
class Counter():
    """Class for comparing counts for a RIIS assessment."""
    def __init__(self, logger):
        """Constructor.

        Args:
            logger (object): logger for saving relevant processing messages
        """
        self._log = logger

    # .............................................................................
    def _get_random_species(
            self, annotated_occ_filename, assessment=LMBISON.INVASIVE_VALUE):
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
        # Count each record for every assessment from annotated occurrences file.
        # Filtered records are retained, but have assessment = ""
        # assessments = {LMBISON.SUMMARY_FILTER_HEADING: 0}
        assessments = {}
        for val in LMBISON.assess_values():
            assessments[val] = 0

        dwcdata = DwcData(annotated_occ_filename, logger)
        try:
            dwcdata.open()

            # Find the species of the first record with riis_assessment
            rec = dwcdata.get_record()
            while rec is not None:
                ass = rec[APPEND_TO_DWC.RIIS_ASSESSMENT]
                # if not ass:
                #     assessments[LMBISON.SUMMARY_FILTER_HEADING] += 1
                # else:
                assessments[ass] += 1
                rec = dwcdata.get_record()
        except Exception as e:
            raise Exception(f"Unknown exception {e} on file {annotated_occ_filename}")
        finally:
            dwcdata.close()

        return assessments

    # .............................................................................
    @classmethod
    def compare_assessment_counts(
            cls, subset_summary_filenames, combined_summary_filename, logger):
        """Compare record counts in subset summaries to counts in the combined summary.

        Args:
            subset_summary_filenames (list): full filename of summary files of
                individual subsets to compare with combined summary file.
            combined_summary_filename (str): full filename of combined summaries.
            logger (object): logger for saving relevant processing messages

        Returns:
            report (dict): dictionary with summaries of count comparisons and lists of
                .error types.
        """
        report = {}
        return report

    # .............................................................................
    @classmethod
    def compare_location_species_counts(
            cls, subset_summary_filenames, combined_summary_filename, logger):
        """Compare record counts in subset summaries to counts in the combined summary.

        Args:
            subset_summary_filenames (list): full filename of summary files of
                individual subsets to compare with combined summary file.
            combined_summary_filename (str): full filename of combined summaries.
            logger (object): logger for saving relevant processing messages

        Returns:
            report (dict): dictionary with summaries of count comparisons and lists of
                .error types.
        """
        # Count each record for every assessment from annotated occurrences file.
        # Filtered records are retained, but have assessment = ""
        report = {}

        sum_locations = {}
        sum_species_keys = {}
        for prefix in REGION.summary_fields().keys():
            sum_locations[prefix] = {}

        for sum_fname in subset_summary_filenames:
            sum_locations, sum_species_keys = cls.count_locations_species(
                sum_fname, locations=sum_locations, species_keys=sum_species_keys)

        cmb_locations, cmb_species_keys = cls.count_locations_species(
            combined_summary_filename)

        unique_loc_key = "Unique locations: Subset <> combined"
        unique_loc_count_errs = []
        for prefix in REGION.summary_fields().keys():
            report[prefix] = {}
            inconsistencies = {}
            counts = {}
            # Check number of unique locations for each prefix (type of location)
            sum_location_num = len(sum_locations[prefix])
            cmb_location_num = len(cmb_locations[prefix])
            if sum_location_num == cmb_location_num:
                counts["unique locations"] = sum_location_num
            else:
                msg = (f"{prefix}: {sum_location_num} <> {cmb_location_num}")
                logger.log(
                    f"{unique_loc_key}: {msg}", refname=cls.__name__,
                    log_level=logging.ERROR)
                unique_loc_count_errs.append(msg)
                inconsistencies[unique_loc_key] = unique_loc_count_errs

            # Check number of occurrences for each prefix (type of location)
            # Contiguous location types should contain all records in the dataset,
            #   except for filtered records.
            missing_key = "Missing from combined summary"
            unequal_key = "Subset summaries <> combined summary"
            missing_errs = []
            unequal_errs = []
            for loc, count in sum_locations[prefix].items():
                sum_msg = (f"Subset summaries {prefix}/{loc}/{count}")
                try:
                    ok = (count == cmb_locations[prefix][loc])
                except KeyError:
                    logger.log(
                        f"{missing_key}: {sum_msg}", refname=cls.__name__,
                        log_level=logging.ERROR)
                    missing_errs.append(sum_msg)
                else:
                    if ok:
                        counts[loc] = count
                    else:
                        cmb_msg = (f"Combined {prefix}/{loc}/{count}")
                        logger.log(f"{unequal_key}: {sum_msg} {cmb_msg}")
                        unequal_errs.append(f"{sum_msg} {cmb_msg}")
            if missing_errs:
                inconsistencies[missing_key] = missing_errs
            if unequal_errs:
                inconsistencies[unequal_key] = unequal_errs
            if inconsistencies:
                report[prefix]["inconsistencies"] = inconsistencies
            if counts:
                report[prefix]["good_counts"] = counts

        return report

    # .............................................................................
    @classmethod
    def count_locations_species(cls, summary_filename, locations=None, species_keys=None):
        """Get count of records for each of the valid location and type in a file.

        Args:
            summary_filename (str): full filename of summary file to total records.
            locations (dict): pre-existing dictionary of location types, with dictionary
                of locations and their counts
            species_keys (dict): pre-existing set of species keys and counts

        Returns:
            assessments (dict): dictionary with keys for each valid assessment type,
                and total record count for each.

        Raises:
            Exception: on unknown open or read error.
        """
        # Count each record for every assessment from annotated occurrences file.
        # Filtered records are retained, but have assessment = ""
        if locations is None:
            locations = {}
            for prefix in REGION.summary_fields().keys():
                locations[prefix] = {}
        if species_keys is None:
            species_keys = {}

        try:
            csv_rdr, inf = get_csv_dict_reader(summary_filename, GBIF.DWCA_DELIMITER)
        except Exception as e:
            raise Exception(f"Failed to open {summary_filename}: {e}")

        try:
            for rec in csv_rdr:
                prefix = rec[LMBISON.LOCATION_TYPE_KEY]
                location = rec[LMBISON.LOCATION_KEY]
                count = int(rec[LMBISON.COUNT_KEY])
                spkey = rec[LMBISON.RR_SPECIES_KEY]
                # scinamekey = rec[LMBISON.SCIENTIFIC_NAME_KEY]
                # spnamekey = rec[LMBISON.SPECIES_NAME_KEY]
                # Count points in each location, ignore species
                try:
                    locations[prefix][location] += count
                except KeyError:
                    locations[prefix][location] = count

                # Count occurrences of unique species keys
                try:
                    species_keys[spkey] += count
                except KeyError:
                    species_keys[spkey] = count

        except Exception as e:
            raise Exception(f"Failed to read {summary_filename}: {e}")
        finally:
            inf.close()
            csv_rdr = None

        return locations, species_keys

    # # .............................................................................
    # def _count_annotated_records_for_species(self, spname, taxkey, state, county):
    #     annotated_filenames = glob.glob(self.annotated_filename_pattern)
    #     state_counts = RIIS_Counts(is_group=False, logger=self._log)
    #     cty_counts = RIIS_Counts(is_group=False, logger=self._log)
    #     for fn in annotated_filenames:
    #         try:
    #             dwcdata = DwcData(fn, self._log)
    #             dwcdata.open()
    #
    #             # Find the species of the first record with riis_assessment
    #             rec = dwcdata.get_record()
    #             while rec is not None:
    #                 if (rec[GBIF.ACC_TAXON_FLD] == taxkey
    #                         and rec[APPEND_TO_DWC.RESOLVED_ST] == state):
    #                     assess = rec[APPEND_TO_DWC.RIIS_ASSESSMENT]
    #                     # Add to state count
    #                     state_counts.add_to(assess, value=1)
    #                     if rec[APPEND_TO_DWC.RESOLVED_CTY] == county:
    #                         # Add to county count
    #                         cty_counts.add_to(assess, value=1)
    #                 rec = dwcdata.get_record()
    #         except Exception as e:
    #             raise Exception(f"Unknown exception {e} on file {fn}")
    #         finally:
    #             dwcdata.close()
    #             dwcdata = None
    #
    #     self._log.log(
    #         f"Counted occurrences of {spname} in {state} and {county} in "
    #         f"{len(annotated_filenames)} annotated files",
    #         refname=self.__class__.__name__)
    #
    #     return state_counts, cty_counts
    #
    # # .............................................................................
    # def _count_annotated_records_for_assessments(self, state, county):
    #     annotated_filenames = glob.glob(self.annotated_filename_pattern)
    #     state_occ_assessment_counts = RIIS_Counts(self._log, is_group=False)
    #     cty_occ_assessment_counts = RIIS_Counts(self._log, is_group=False)
    #     # Track species for each assessment in county and state
    #     cty_species = {}
    #     state_species = {}
    #     for ass in LMBISON.ASSESS_VALUES:
    #         cty_species[ass] = set()
    #         state_species[ass] = set()
    #
    #     for fn in annotated_filenames:
    #         try:
    #             dwcdata = DwcData(fn, self._log)
    #             dwcdata.open()
    #
    #             # Find the species of the first record with riis_assessment
    #             rec = dwcdata.get_record()
    #             while rec is not None:
    #                 assess = rec[APPEND_TO_DWC.RIIS_ASSESSMENT]
    #                 taxkey = rec[GBIF.ACC_TAXON_FLD]
    #                 if rec[APPEND_TO_DWC.RESOLVED_ST] == state:
    #                     # Add to occ count
    #                     state_occ_assessment_counts.add_to(assess, value=1)
    #                     # Add to set of species
    #                     state_species[assess].add(taxkey)
    #                     if county is not None and rec[APPEND_TO_DWC.RESOLVED_CTY] == county:
    #                         # Add to occ count
    #                         cty_occ_assessment_counts.add_to(assess, value=1)
    #                         # Add to set of species
    #                         cty_species[assess].add(taxkey)
    #
    #                 rec = dwcdata.get_record()
    #         except Exception as e:
    #             raise Exception(f"Unknown exception {e} on file {fn}")
    #         finally:
    #             dwcdata.close()
    #             dwcdata = None
    #
    #     state_species_counts = RIIS_Counts(
    #         self._log, introduced=len(state_species["introduced"]),
    #         invasive=len(state_species["invasive"]),
    #         presumed_native=len(state_species["presumed_native"]),
    #         is_group=True)
    #     cty_species_counts = RIIS_Counts(
    #         self._log, introduced=len(cty_species["introduced"]),
    #         invasive=len(cty_species["invasive"]),
    #         presumed_native=len(cty_species["presumed_native"]),
    #         is_group=True)
    #
    #     self._log.log(
    #         f"Counted species for assessments in {state} and {county} in "
    #         f"{len(annotated_filenames)} annotated files", refname=self.__class__.__name__)
    #
    #     return state_occ_assessment_counts, cty_occ_assessment_counts, state_species_counts, cty_species_counts
    #
    # # .............................................................................
    # def _get_assess_summary(self, state, county=None, is_group=False):
    #     species_counts = occ_counts = None
    #     assess_summary_fname = BisonNameOp.construct_assessment_summary_name(self._datapath)
    #     try:
    #         rdr, inf = get_csv_dict_reader(assess_summary_fname, GBIF.DWCA_DELIMITER)
    #         for rec in rdr:
    #             if rec[LMBISON.STATE_KEY] == state:
    #                 # occurrence counts
    #                 intro_occ = rec[LMBISON.INTRODUCED_OCCS]
    #                 inv_occ = rec[LMBISON.INVASIVE_OCCS]
    #                 native_occ = rec[LMBISON.NATIVE_OCCS]
    #                 # species/group counts
    #                 intro_sp = rec[LMBISON.INTRODUCED_SPECIES]
    #                 inv_sp = rec[LMBISON.INVASIVE_SPECIES]
    #                 native_sp = rec[LMBISON.NATIVE_SPECIES]
    #                 if county is None:
    #                     if not rec[LMBISON.COUNTY_KEY]:
    #                         species_counts = RIIS_Counts(
    #                             self._log, introduced=intro_sp, invasive=inv_sp,
    #                             presumed_native=native_sp, is_group=True)
    #                         occ_counts = RIIS_Counts(
    #                             self._log, introduced=intro_occ, invasive=inv_occ,
    #                             presumed_native=native_occ, is_group=False)
    #                         break
    #                 elif rec[LMBISON.COUNTY_KEY] == county:
    #                     species_counts = RIIS_Counts(
    #                         self._log, introduced=intro_sp, invasive=inv_sp,
    #                         presumed_native=native_sp, is_group=True)
    #                     occ_counts = RIIS_Counts(
    #                         self._log, introduced=intro_occ, invasive=inv_occ,
    #                         presumed_native=native_occ, is_group=False)
    #                     break
    #     except Exception as e:
    #         raise Exception(
    #             f"Unexpected error {e} in opening or reading {assess_summary_fname}")
    #     return (species_counts, occ_counts)
    #
    # # .............................................................................
    # def _get_region_count(self, acc_species_name, state, county=None):
    #     region_summary_fname = Aggregator.construct_location_summary_name(
    #         self._datapath, state, county=county)
    #     # Get counts for all assessments of species_key in this region summary file
    #     loc_occ_counts = RIIS_Counts(self._log, is_group=False)
    #     try:
    #         rdr, inf = get_csv_dict_reader(region_summary_fname, GBIF.DWCA_DELIMITER)
    #     except Exception as e:
    #         raise Exception(f"Unexpected open error {e} in {region_summary_fname}")
    #
    #     try:
    #         for rec in rdr:
    #             if rec[GBIF.ACC_NAME_FLD] == acc_species_name:
    #                 rass = rec[LMBISON.ASSESS_KEY]
    #                 count = int(rec[LMBISON.COUNT_KEY])
    #                 if rass == "introduced":
    #                     loc_occ_counts.introduced = count
    #                 elif rass == "invasive":
    #                     loc_occ_counts.invasive = count
    #                 elif rass == "presumed_native":
    #                     loc_occ_counts.presumed_native = count
    #                 else:
    #                     raise Exception(f"Unknown record field {rass}.")
    #                 break
    #     except Exception as e:
    #         raise Exception(f"Unexpected read error {e} in {region_summary_fname}")
    #     finally:
    #         inf.close()
    #
    #     self._log.log(
    #         f"Read occurrence summary for {acc_species_name} in "
    #         f"{region_summary_fname} summary file", refname=self.__class__.__name__)
    #
    #     return loc_occ_counts
    #
    # # .............................................................................
    # def _log_comparison(self, truth_counts, summary_counts, compare_type, source):
    #     self._log.log(
    #         f"Compare annotation counts to {source} ({compare_type}) introduced, "
    #         f"invasive, presumed_native: ", refname=self.__class__.__name__)
    #     self._log.log(
    #         f"    {truth_counts.introduced}, {truth_counts.invasive}, "
    #         f"{truth_counts.presumed_native}", refname=self.__class__.__name__)
    #     self._log.log(
    #         f"    {summary_counts.introduced}, {summary_counts.invasive}, "
    #         f"{summary_counts.presumed_native}", refname=self.__class__.__name__)
    #     if truth_counts.equals(summary_counts):
    #         self._log.log("Success!", refname=self.__class__.__name__)
    #     else:
    #         self._log.log(
    #             "FAIL! Annotations do not match summaries",
    #             refname=self.__class__.__name__)
    #     self._log.log("", refname=self.__class__.__name__)
    #
    # # .............................................................................
    # def compare_counts(self):
    #     """Compare matching annotated records against the counts in summary files."""
    #     # Get an invasive species occurrence from one annotated file
    #     filenames = glob.glob(self.annotated_filename_pattern)
    #     midx = int(len(filenames) / 2)
    #     acc_species_name, taxkey, county, state = self._get_random_species(
    #         filenames[midx], "invasive")
    #
    #     self._log.log(
    #         "--------------------------------------", refname=self.__class__.__name__)
    #     self._log.log(
    #         f"Compare `ground truth` occurrences of '{acc_species_name}' in "
    #         f"{county} {state} to region summaries", refname=self.__class__.__name__)
    #     self._log.log(
    #         "--------------------------------------", refname=self.__class__.__name__)
    #
    #     # Ground truth: Count matching lines for species and region in annotated records
    #     gtruth_state_occXspecies, gtruth_county_occXspecies = \
    #         self._count_annotated_records_for_species(
    #             acc_species_name, taxkey, state, county)
    #
    #     # Counts from state and county summary files
    #     state_loc_occ_counts = self._get_region_count(
    #         acc_species_name, state, county=None)
    #     county_loc_occ_counts = self._get_region_count(
    #         acc_species_name, state, county=county)
    #
    #     # Compare
    #     self._log_comparison(
    #         gtruth_state_occXspecies, state_loc_occ_counts,
    #         f"{state} {acc_species_name} occurrence", f"{state} summary")
    #     self._log_comparison(
    #         gtruth_county_occXspecies, county_loc_occ_counts,
    #         f"{county} {acc_species_name} occurrence", f"{county} summary")
    #
    #     self._log.log(
    #         "--------------------------------------", refname=self.__class__.__name__)
    #     self._log.log(
    #         f"Compare `ground truth` assessment (occurrences) {county} {state} to " +
    #         "RIIS summary", refname=self.__class__.__name__)
    #     self._log.log(
    #         "--------------------------------------", refname=self.__class__.__name__)
    #
    #     # Ground truth: Count matching lines for assessment and region in annotated recs
    #     (gtruth_state_occXassess, gtruth_cty_occXassess, gtruth_state_speciesXassess,
    #         gtruth_cty_speciesXassess) = self._count_annotated_records_for_assessments(
    #         state, county)
    #
    #     # Counts from RIIS assessment summary
    #     (st_species_counts, st_occ_counts) = self._get_assess_summary(state)
    #     (cty_species_counts, cty_occ_counts) = self._get_assess_summary(
    #         state, county=county)
    #
    #     # Compare
    #     self._log_comparison(
    #         gtruth_state_occXassess, st_occ_counts,
    #         f"{state} occurrence", "RIIS summary")
    #     self._log_comparison(
    #         gtruth_cty_occXassess, cty_occ_counts,
    #         f"{county} occurrence", "RIIS summary")
    #
    #     # Compare species counts
    #     self._log.log(
    #         "--------------------------------------", refname=self.__class__.__name__)
    #     self._log.log(
    #         f"Compare `ground truth` assessment (species) {county} {state} to RIIS "
    #         f"summary", refname=self.__class__.__name__)
    #     self._log.log(
    #         "--------------------------------------", refname=self.__class__.__name__)
    #     self._log_comparison(
    #         gtruth_state_speciesXassess, st_species_counts,
    #         f"{state} species", "RIIS summary")
    #     self._log_comparison(
    #         gtruth_cty_speciesXassess, cty_species_counts,
    #         f"{county} species", "RIIS summary")


# .............................................................................
__all__ = ["Counter"]
