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
SUMMARY_HEADER = [LOCATION_KEY, SPECIES_KEY, COUNT_KEY]

# Data Product 1: For each state or county file, 3 fields, for each species: species, count, assessment
STATE_COUNTY_HEADER = [SPECIES_KEY, COUNT_KEY, ASSESS_KEY]

# Data Product 2: For aggregate file, 7 fields, for each county/state and state:
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
        try:
            part1, part2 = compound_key.split(AGGREGATOR_DELIMITER)
        except ValueError:
            raise ValueError(f"Unexpected compound_key {compound_key} does not parse into 2 parts")
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
            county, state = self._parse_compound_key(location)
        except ValueError as e:
            self._log.error(f"{e}")
            pass
        else:
            if state and county:
                self.states[state].add(county)
            else:
                self._log.error(f"Why no state {state} or county {county}?")

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
                county_state = self._get_compound_key(county, state)

                self._add_record_to_location_summaries(county_state, species_key)
                self._add_record_to_location_summaries(state, species_key)

        except Exception as e:
            raise Exception(f"Failed to read annotated occurrences from {self._csvfile}: {e}")

        finally:
            csv_rdr = None
            inf.close()

    # ...............................................
    def _write_summary(self, summary_filename):
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
    def summarize_write_locations(self):
        """Read an annotated file, summarize by species and location, write to a csvfile.

        Returns:
            summary_filename: full output filename for CSV summary of annotated occurrence file. CSV fields are
                 [SPECIES_KEY, GBIF_TAXON_KEY, ASSESS_KEY, STATE_KEY, COUNTY_KEY, COUNT_KEY].
        """
        summary_filename = self.construct_summary_name(self._csvfile)
        # Summarize and write
        self._summarize_annotations_by_location()
        self._write_summary(summary_filename)
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
            summary_filename = os.path.join(datapath, "out", f"state_{state}.csv")
        else:
            location = self._get_compound_key(county, state)
            summary_filename = os.path.join(datapath, "out", f"county_{county}_{state}.csv")

        if state in ("AK", "HI"):
            region = state
        else:
            region = "L48"

        try:
            species_counts = self.locations[location]
        except KeyError:
            self._log.warn(f"No species occurrences found for state {state}, county {county}")
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
    def aggregate_write_for_locations(self, summary_filename_list):
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

        state_aggregation_filenames = set()
        cty_aggregation_filenames = set()
        nnsl = self._get_riis_species()

        for state, counties in self.states.items():
            state_agg_fname = self._write_statecounty_aggregate(datapath, nnsl, state)
            state_aggregation_filenames.add(state_agg_fname)
            for county in counties:
                cty_agg_fname = self._write_statecounty_aggregate(datapath, nnsl, state, county=county)
                cty_aggregation_filenames.add(cty_agg_fname)

        return state_aggregation_filenames, cty_aggregation_filenames
