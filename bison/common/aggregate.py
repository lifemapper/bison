"""Common classes for adding USGS RIIS info to GBIF occurrences."""
import os

from bison.common.constants import (
    AGGREGATOR_DELIMITER, ENCODING, GBIF, NEW_RESOLVED_COUNTY, NEW_RESOLVED_STATE, RIIS_SPECIES, US_STATES)
from bison.common.riis import NNSL

from bison.tools.util import (get_csv_writer, get_csv_dict_reader, get_logger)

# For each summary file, 3 fields, for each location: location, species, count
# Summary files are used only internally, to process multiple smaller files, then read summaries for output
SPECIES_KEY = "species_key"
ASSESS_KEY = "assessment"
STATE_KEY = "state"
COUNTY_KEY = "county"
LOCATION_KEY = "location"
COUNT_KEY = "count"
PERCENT_KEY = "percent_of_total"
SUMMARY_HEADER = [LOCATION_KEY, SPECIES_KEY, COUNT_KEY]

# Data Product 1: For each state or county file, 3 fields, for each species: species, count, assessment
STATE_COUNTY_HEADER = [SPECIES_KEY, COUNT_KEY, ASSESS_KEY]
RIIS_HEADER = [STATE_KEY, COUNTY_KEY, ASSESS_KEY, COUNT_KEY, PERCENT_KEY]
RIIS_HEADER = [
    STATE_KEY, COUNTY_KEY,
    "introduced_species", "invasive_species",  "all_species",
    "pct_introduced_all_species", "pct_invasive_all_species",
    "introduced_occurrences", "invasive_occurrences", "all_occurrences",
    "pct_introduced_all_occurrences", "pct_invasive_all_occurrences"]
#                                location,
#                                bad # species, total # species, bad/total % species,
#                                bad # occurrences, total # occurrences, bad/total % occurrences
BAD_SPECIES_COUNT = "introduced_species_count"
TOTAL_SPECIES_COUNT = "total_species_count"
BAD_SPECIES_PERCENT = "percent_introduced_to_total_species"
BAD_OCC_COUNT = "introduced_occurrence_count"
TOTAL_OCC_COUNT = "total_occurrence_count"
BAD_OCC_PERCENT = "percent_introduced_to_total_occurrences"
AGGREGATE_HEADER = [
    LOCATION_KEY, BAD_SPECIES_COUNT, TOTAL_SPECIES_COUNT, BAD_SPECIES_PERCENT,
    BAD_OCC_COUNT, TOTAL_OCC_COUNT, BAD_OCC_PERCENT]


# .............................................................................
class Aggregator():
    """Class for summarizing GBIF data annotated with USGS Introduced and Invasive species assessments.

    Goal:
     Finally, we will summarize the results of data processing.
     * One set of results will include a list of “introduced” species and a list of “presumed native” species for
       each state and county, along with counts for each of these species.
     * The second set of results will show the summary counts of introduced species and all species,
       introduced occurrences and all occurrences, the percentage of introduced to all species, the percentage of
       introduced to all occurrences, all by state and county
    """
    def __init__(self, annotated_filename, logger=None):
        """Constructor.

        Args:
            annotated_filename (str): full filename for annotated occurrence CSV file
            logger (object): logger for saving relevant processing messages

        Raises:
            FileNotFoundError: on any missing file in occ_filename_list
        """
        if not os.path.exists(annotated_filename):
            raise FileNotFoundError(f"File {annotated_filename} does not exist")

        datapath, _ = os.path.split(annotated_filename)
        self._datapath = datapath
        self._csvfile = annotated_filename
        if logger is None:
            logger = get_logger(datapath)
        # Hold all counties found in each state
        self.states = {}
        self._init_states()

        self._log = logger
        # {county_or_state: {species: count, ... }
        #  ...  }
        self.locations = {}

    # ...............................................
    def _init_states(self):
        self.states = {}
        for st in US_STATES.values():
            self.states[st] = set()

    # ...............................................
    def _get_compound_key(self, part1, part2):
        if part1 and part2:
            return f"{part1}{AGGREGATOR_DELIMITER}{part2}"
        else:
            self._log.error(f"Why missing part1 {part1} or part2 {part2}")

    # ...............................................
    def _parse_compound_key(self, compound_key):
        parts = compound_key.split(AGGREGATOR_DELIMITER)
        part1 = parts[0]
        if len(parts) == 2:
            part2 = parts[1]
        elif len(parts) == 1:
            part2 = None
        else:
            raise ValueError(f"Unexpected compound_key {compound_key}")
        return part1, part2

    # ...............................................
    @classmethod
    def construct_summary_name(cls, csvfile):
        """Construct a filename for the summarized version of csvfile.

        Args:
            csvfile (str): full filename used to construct an annotated filename for this data.

        Returns:
            outfname: output filename derived from the annotated GBIF DWC filename
        """
        basename, ext = os.path.splitext(csvfile)
        outfname = f"{basename}_summary{ext}"
        return outfname

    # ...............................................
    @classmethod
    def construct_location_summary_name(cls, datapath, state, county=None):
        """Construct a filename for the summary file for a particular state and optional county.

        Args:
            datapath (str): full directory path for computations and output.
            state (str): 2-character ISO state code
            county (str): optional county for summary

        Returns:
            outfname: output filename derived from the state and county
        """
        if county is None:
            basename = f"state_{state}.csv"
        else:
            basename = f"county_{state}_{county}.csv"
        outfname = os.path.join(datapath, "out", basename)
        return outfname

    # ...............................................
    @classmethod
    def parse_location_summary_name(cls, csvfile):
        """Construct a filename for the summarized version of csvfile.

        Args:
            csvfile (str): full filename used to construct an annotated filename for this data.

        Returns:
            outfname: output filename derived from the annotated GBIF DWC filename

        Raises:
            Exception: on filename does not start with "state_" or "county_"
        """
        county = state = None
        _, basefilename = os.path.split(csvfile)
        basename, ext = os.path.splitext(basefilename)
        if basename.startswith("state_"):
            _, state = basename.split("_")
        elif basename.startswith("county_"):
            _, state, county = basename.split("_")
        else:
            raise Exception(f"Filename {csvfile} cannot be parsed into location elements")
        return state, county

    # ...............................................
    def _add_record_to_location_summaries(self, location, species_key, count=1):
        # locations = {county_or_state: {species: count,  ... } ... }
        try:
            count = int(count)
        except ValueError:
            raise ValueError(f"Species count {count} must be an integer")
        # Add to summary of unique locations
        try:
            # Is location present?
            self.locations[location]
            try:
                # Is species present in location?
                self.locations[location][species_key] += count
            except KeyError:
                # Missing species in location, add species to location
                self.locations[location][species_key] = count

        except KeyError:
            # Location not found, add location with species
            self.locations[location] = {species_key: count}

        # Add to summary of counties by state
        try:
            state, county = self._parse_compound_key(location)
        except ValueError:
            raise
        else:
            if county is not None:
                self.states[state].add(county)

    # ...............................................
    def _summarize_annotations_by_location(self):
        # Reset summary
        self.locations = {}
        csv_rdr, inf = get_csv_dict_reader(self._csvfile, GBIF.DWCA_DELIMITER, encoding=ENCODING)
        try:
            for rec in csv_rdr:
                species_key = self._get_compound_key(rec[GBIF.ACC_TAXON_FLD], rec[GBIF.ACC_NAME_FLD])
                state = rec[NEW_RESOLVED_STATE]
                county = rec[NEW_RESOLVED_COUNTY]
                county_state = self._get_compound_key(state, county)

                self._add_record_to_location_summaries(county_state, species_key)
                self._add_record_to_location_summaries(state, species_key)

        except Exception as e:
            raise Exception(f"Failed to read annotated occurrences from {self._csvfile}: {e}")

        finally:
            csv_rdr = None
            inf.close()

    # ...............................................
    def _write_location_summary(self, summary_filename):
        # locations = {county_or_state: {species: count,
        #                                ...}
        #              ...}
        # header = [LOCATION_KEY, SPECIES_KEY, COUNT_KEY]
        try:
            csv_wtr, outf = get_csv_writer(summary_filename, GBIF.DWCA_DELIMITER, fmode="w")
            csv_wtr.writerow(SUMMARY_HEADER)

            # Location keys are state and county_state
            for location, sp_info in self.locations.items():
                for species, count in sp_info.items():
                    row = [location, species, count]
                    csv_wtr.writerow(row)
        except Exception as e:
            raise Exception(f"Failed to write summary file {summary_filename}: {e}")
        finally:
            outf.close()

    # ...............................................
    def read_location_aggregates(self, summary_filename_list):
        """Read summary files and combine summaries in self.locations.

        Args:
            summary_filename_list: list of full filenames of summary files.

        Raises:
            Exception: on failure to open or read a file.
        """
        for sum_fname in summary_filename_list:
            try:
                csv_rdr, inf = get_csv_dict_reader(sum_fname, GBIF.DWCA_DELIMITER)
                for rec in csv_rdr:
                    self._add_record_to_location_summaries(rec[LOCATION_KEY], rec[SPECIES_KEY], count=rec[COUNT_KEY])
            except Exception as e:
                raise Exception(f"Failed to open or read {sum_fname}: {e}")
            finally:
                inf.close()

    # ...............................................
    def summarize_by_file(self):
        """Read an annotated file, summarize by species and location, write to a csvfile.

        Returns:
            summary_filename: full output filename for CSV summary of annotated occurrence file. CSV fields are
                 [SPECIES_KEY, GBIF_TAXON_KEY, ASSESS_KEY, STATE_KEY, COUNTY_KEY, COUNT_KEY].
        """
        summary_filename = self.construct_summary_name(self._csvfile)
        # Summarize and write
        self._summarize_annotations_by_location()
        self._write_location_summary(summary_filename)
        return summary_filename

    # # ...............................................
    # def _summarize_summaries(self, summary_filename_list):
    #     """Read annotated data from summary files and populate the self.locations dictionary.
    #
    #     Args:
    #         summary_filename_list (list): list of full summary input filenames to be read.
    #
    #     Raises:
    #         Exception: on unexpected read error
    #
    #     Note: [LOCATION_KEY, SPECIES_KEY, COUNT_KEY]
    #     """
    #     self.locations = {}
    #     # Read summaries of occurrences and aggregate
    #     for summary_filename in summary_filename_list:
    #         try:
    #             csv_rdr, inf = get_csv_dict_reader(summary_filename, GBIF.DWCA_DELIMITER, encoding=ENCODING)
    #             for rec in csv_rdr:
    #                 self._add_record_to_location_summaries(rec[LOCATION_KEY], rec[SPECIES_KEY], count=rec[COUNT_KEY])
    #         except Exception as e:
    #             raise Exception(f"Failed to read summary file {summary_filename}: {e}")
    #         finally:
    #             inf.close()

    # ...............................................
    def _get_riis_species(self):
        riis_filename = os.path.join(self._datapath, RIIS_SPECIES.FNAME)
        nnsl = NNSL(riis_filename, logger=self._log)
        nnsl.read_riis(read_resolved=True)
        return nnsl

    # ...............................................
    def _examine_species_for_location(self, region, species_counts, nnsl):
        recs = []
        if species_counts:
            # All these species are for the location of interest
            for species_key, count in species_counts.items():
                try:
                    gbif_taxon_key, species = self._parse_compound_key(species_key)
                except ValueError:
                    raise (f"Error in species_key {species_key}, should contain name and taxonKey")

                assessments = nnsl.get_assessments_for_gbif_taxonkey(gbif_taxon_key)
                try:
                    assess = assessments[region]
                except KeyError:
                    assess = "presumed_native"

                recs.append([species, count, assess])
        return recs

    # ...............................................
    def _write_statecounty_aggregate(self, datapath, nnsl, state, county=None):
        """Summarize aggregated data for a location by species.

        Args:
            datapath (str): output path for aggregate file
            nnsl (bison.common.riis.NNSL): Non-native species list object
            state (str): 2-character state code
            county (str): County name from census data.

        Returns:
            summary_filename: output file

        Raises:
            Exception: on unexpected write error

        [SPECIES_KEY, COUNT_KEY, ASSESS_KEY]
        """
        if county is None:
            location = state
            summary_filename = self.construct_location_summary_name(datapath, state)
        else:
            location = self._get_compound_key(state, county)
            summary_filename = self.construct_location_summary_name(datapath, state, county=county)

        if state in ("AK", "HI"):
            region = state
        else:
            region = "L48"

        try:
            species_counts = self.locations[location]
        except KeyError:
            self._log.warn(f"No species occurrences found for {summary_filename}")
            summary_filename = None
        else:
            try:
                csv_wtr, outf = get_csv_writer(summary_filename, GBIF.DWCA_DELIMITER, fmode="w")
                csv_wtr.writerow(STATE_COUNTY_HEADER)
                # Write all records found
                records = self._examine_species_for_location(region, species_counts, nnsl)
                for rec in records:
                    csv_wtr.writerow(rec)
            except Exception as e:
                raise Exception(f"Unknown write error on {summary_filename}: {e}")
            finally:
                outf.close()

        return summary_filename

    # ...............................................
    def _summarize_location_by_riis(csvwriter, loc_summary_file, state, county=None):
        # read [SPECIES_KEY, COUNT_KEY, ASSESS_KEY]
        # write [STATE_KEY, COUNTY_KEY,
        #     "introduced_species", "invasive_species",  "all_species",
        #     "pct_introduced_all_species", "pct_invasive_all_species",
        #     "introduced_occurrences", "invasive_occurrences", "all_occurrences",
        #     "pct_introduced_all_occurrences", "pct_invasive_all_occurrences"]
        assess = {"introduced": {}, "invasive": {}, "presumed_native": {}}
        try:
            rdr, inf = get_csv_dict_reader(loc_summary_file, GBIF.DWCA_DELIMITER, encoding=ENCODING, quote_none=False)
            for rec in rdr:
                assess[rec[ASSESS_KEY]][rec[SPECIES_KEY]] = rec[COUNT_KEY]
        except Exception as e:
            raise Exception(f"Unknown read error on {loc_summary_file}: {e}")
        finally:
            inf.close()

        species_total = 0
        occ_total = 0
        for ass_key, sp_counts in assess.items():
            ass_occ_total = 0
            ass_species_total = 0
            for _, count in sp_counts.items():
                ass_species_total += 1
                ass_occ_total += count
            # Count all
            species_total += ass_species_total
            occ_total += ass_occ_total
            # Count introduced/invasive
            if ass_key == "introduced":
                intro_species_total = ass_species_total
                intro_occ_total = ass_occ_total
            elif ass_key == "invasive":
                invasive_species_total = ass_species_total
                invasive_occ_total = ass_occ_total
        pct_invasive_species = (invasive_species_total / species_total)
        pct_intro_species = (intro_species_total / species_total)
        pct_invasive_occ = (invasive_occ_total / occ_total)
        pct_intro_occ = (intro_occ_total / occ_total)

        csvwriter.writerow([
            state, county,
            intro_species_total, invasive_species_total, species_total, pct_intro_species, pct_invasive_species,
            intro_occ_total, invasive_occ_total, occ_total, pct_intro_occ, pct_invasive_occ
        ])

    # ...............................................
    def summarize_by_location(self, summary_filename_list):
        """Read annotated data from one or more files, summarize by species, and by location.

        Args:
            summary_filename_list (list): list of full summary input filenames to be read.

        Returns:
            state_aggregation_filenames (list): full filenames of species counts and percentages for each state.
            cty_aggregation_filename (list): full filenames of species counts and percentages for each county-state.
        """
        self.locations = {}
        datapath, _ = os.path.split(summary_filename_list[0])
        self.read_location_aggregates(summary_filename_list)

        location_aggregation_filenames = set()
        nnsl = self._get_riis_species()

        for state, counties in self.states.items():
            state_agg_fname = self._write_statecounty_aggregate(datapath, nnsl, state)
            location_aggregation_filenames.add(state_agg_fname)
            for county in counties:
                cty_agg_fname = self._write_statecounty_aggregate(datapath, nnsl, state, county=county)
                location_aggregation_filenames.add(cty_agg_fname)

        return location_aggregation_filenames

    # ...............................................
    def summarize_by_assessment(self, summary_filename_list):
        """Read annotated data from one or more files, summarize by species, and by location.

        Args:
            summary_filename_list (list): list of full summary input filenames for which to aggregate totals by RIIS.

        Returns:
            summary_filename (str): full filenames of introduced, invasive, presumed_native counts and percentages for
                each county-state.

        Raises:
            Exception: on unexpected open or write error

        [STATE_KEY, COUNTY_KEY, ASSESS_KEY, COUNT_KEY, PERCENT_KEY]
        """
        self.locations = {}
        for _fname in summary_filename_list:
            break
        datapath, _ = os.path.split(_fname)
        summary_filename = os.path.join(datapath, "out", "riis_summary.csv")
        try:
            csvwtr, outf = get_csv_writer(summary_filename, GBIF.DWCA_DELIMITER, fmode="w")
            csvwtr.writerow(RIIS_HEADER)

            # Summarize introduced/invasive/native in each location
            for locfile in summary_filename_list:
                state, county = self.parse_location_summary_name(locfile)
                self._summarize_location_by_riis(csvwtr, locfile, state, county=county)

        except Exception as e:
            raise Exception(f"Unknown write error on {summary_filename}: {e}")
        finally:
            outf.close()

        return summary_filename
