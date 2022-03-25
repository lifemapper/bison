"""Common classes for adding USGS RIIS info to GBIF occurrences."""
import os

from bison.common.constants import (
    AGGREGATOR_DELIMITER, ENCODING, GBIF, NEW_RESOLVED_COUNTY, NEW_RESOLVED_STATE, RIIS_SPECIES, US_STATES)
from bison.common.riis import NNSL

from bison.tools.util import (get_csv_writer, get_csv_dict_reader, get_logger)

# For each temporary summary file, 3 fields, for each location: location, species, count
# Summary files are used to individually process manageably-sized files, then read and aggregate summaries for final output
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
# Data Product 2: For each state and county, 1 record with counts and percents of
#                 introduced, invasive, presumed_native occurrences and species
RIIS_HEADER = [
    STATE_KEY, COUNTY_KEY,
    "introduced_species", "invasive_species",  "presumed_native_species", "all_species",
    "pct_introduced_all_species", "pct_invasive_all_species", "pct_presumed_native_species",
    "introduced_occurrences", "invasive_occurrences", "presumed_native_occurrences", "all_occurrences",
    "pct_introduced_all_occurrences", "pct_invasive_all_occurrences", "pct_presumed_native_occurrences"]

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
    def _summarize_annotations_by_region(self):
        # Reset summary
        self.locations = {}
        csv_rdr, inf = get_csv_dict_reader(self._csvfile, GBIF.DWCA_DELIMITER, encoding=ENCODING)
        self._log.info(f"Summarizing annotations in {self._csvfile} by region")
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
    def _write_region_summary(self, summary_filename):
        # locations = {county_or_state: {species: count,
        #                                ...}
        #              ...}
        # header = [LOCATION_KEY, SPECIES_KEY, COUNT_KEY]
        try:
            csv_wtr, outf = get_csv_writer(summary_filename, GBIF.DWCA_DELIMITER, fmode="w")
            csv_wtr.writerow(SUMMARY_HEADER)
            self._log.info(f"Writing region summaries to {summary_filename}")

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
        self._summarize_annotations_by_region()
        self._write_region_summary(summary_filename)
        self._log.info(f"Summarized species by region from {self._csvfile} to {summary_filename}")
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
                    assess = assessments[region].lower()
                except KeyError:
                    assess = "presumed_native"

                recs.append([species, count, assess])
        return recs

    # ...............................................
    def _write_region_aggregate(self, datapath, nnsl, state, county=None):
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
                self._log.info(f"Wrote region summaries to {summary_filename}")
            except Exception as e:
                raise Exception(f"Unknown write error on {summary_filename}: {e}")
            finally:
                outf.close()

        return summary_filename

    # ...............................................
    def _summarize_region_by_riis(self, csvwriter, loc_summary_file, state, county=None):
        # read [SPECIES_KEY, COUNT_KEY, ASSESS_KEY]
        # write [STATE_KEY, COUNTY_KEY,
        #     "introduced_species", "invasive_species",  "presumed_native_species", "all_species",
        #     "pct_introduced_all_species", "pct_invasive_all_species", "pct_presumed_native_species",
        #     "introduced_occurrences", "invasive_occurrences", "presumed_native_occurrences", "all_occurrences",
        #     "pct_introduced_all_occurrences", "pct_invasive_all_occurrences", "pct_presumed_native_occurrences"]
        assess = {"introduced": {}, "invasive": {}, "presumed_native": {}}
        try:
            rdr, inf = get_csv_dict_reader(loc_summary_file, GBIF.DWCA_DELIMITER, encoding=ENCODING, quote_none=False)
            for rec in rdr:
                try:
                    count = int(rec[COUNT_KEY])
                except ValueError:
                    raise
                assess[rec[ASSESS_KEY]][rec[SPECIES_KEY]] = count
        except Exception as e:
            raise Exception(f"Unknown read error on {loc_summary_file}: {e}")
        finally:
            inf.close()

        species_total = occ_total = 0
        # Count introduced/invasive
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
            elif ass_key == "presumed_native":
                native_species_total = ass_species_total
                native_occ_total = ass_occ_total

        assessed_sp_total = intro_species_total + invasive_species_total + native_species_total
        if assessed_sp_total != species_total:
            self._log.info(f"Assessed species totals for {state} {county}: {assessed_sp_total} <> {species_total}")
        assessed_occ_total = intro_occ_total + invasive_occ_total + native_occ_total
        if assessed_occ_total != occ_total:
            self._log.info(f"Assessed occurrence totals for {state} {county}: {assessed_occ_total} <> {occ_total}")

        pct_intro_species = (intro_species_total / species_total)
        pct_invasive_species = (invasive_species_total / species_total)
        pct_native_species = (native_species_total / species_total)
        pct_species = pct_intro_species + pct_invasive_species + pct_native_species
        if pct_species != 1:
            self._log.info(f"Percent species totals for {state} {county}: {pct_species} <> 1.0")

        pct_intro_occ = (intro_occ_total / occ_total)
        pct_invasive_occ = (invasive_occ_total / occ_total)
        pct_native_occ = (native_occ_total / occ_total)
        pct_occ = pct_intro_occ + pct_invasive_occ + pct_native_occ
        if pct_occ != 1:
            self._log.info(f"Percent occ totals for {state} {county}: {pct_occ} <> 1.0")

        csvwriter.writerow([
            state, county,
            intro_species_total, invasive_species_total, native_species_total, species_total,
            pct_intro_species, pct_invasive_species, pct_native_species,
            intro_occ_total, invasive_occ_total, native_occ_total, occ_total,
            pct_intro_occ, pct_invasive_occ, pct_native_occ
        ])

        return (
            intro_species_total, invasive_species_total, native_species_total, species_total,
            intro_occ_total, invasive_occ_total, native_occ_total, occ_total)

    # ...............................................
    def summarize_regions(self, summary_filename_list):
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
        self._log.info("Read summarized annotations to aggregate by region")

        region_summary_filenames = []
        nnsl = self._get_riis_species()

        for state, counties in self.states.items():
            state_agg_fname = self._write_region_aggregate(datapath, nnsl, state)
            if state_agg_fname is not None:
                region_summary_filenames.append(state_agg_fname)
            for county in counties:
                cty_agg_fname = self._write_region_aggregate(datapath, nnsl, state, county=county)
                if cty_agg_fname is not None:
                    region_summary_filenames.append(cty_agg_fname)

        return region_summary_filenames

    # ...............................................
    def summarize_assessments(self, region_summary_filenames):
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
        for _fname in region_summary_filenames:
            break
        datapath, _ = os.path.split(_fname)
        assess_summary_filename = os.path.join(datapath, "riis_summary.csv")
        try:
            csvwtr, outf = get_csv_writer(assess_summary_filename, GBIF.DWCA_DELIMITER, fmode="w")
            csvwtr.writerow(RIIS_HEADER)
        except Exception as e:
            raise Exception(f"Unknown open/csv error on {assess_summary_filename}: {e}")

        intro_species_total = invasive_species_total = native_species_total = species_total = 0
        intro_occ_total = invasive_occ_total = native_occ_total = occ_total = 0
        try:
            # Summarize introduced/invasive/native in each location
            for region_file in region_summary_filenames:
                state, county = self.parse_location_summary_name(region_file)
                (intro_species, invasive_species, native_species, species, intro_occ, invasive_occ, native_occ,
                    occ) = self._summarize_region_by_riis(csvwtr, region_file, state, county=county)

                intro_species_total += intro_species
                invasive_species_total += invasive_species
                native_species_total += native_species
                species_total += species
                intro_occ_total += intro_occ
                invasive_occ_total += invasive_occ
                native_occ_total += native_occ
                occ_total += occ

            assessed_sp_total = intro_species_total + invasive_species_total + native_species_total
            self._log.info(f"Assessed species totals {assessed_sp_total} ?= {species_total}")
            assessed_occ_total = intro_occ_total + invasive_occ_total + native_occ_total
            self._log.info(f"Assessed occurrence totals {assessed_occ_total} ?= {occ_total}")

            self._log.info(f"Wrote RIIS assessment summaries to {assess_summary_filename}")
        except Exception as e:
            raise Exception(f"Unknown write error on {assess_summary_filename}: {e}")
        finally:
            outf.close()

        return assess_summary_filename
