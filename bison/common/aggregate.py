"""Common classes for adding USGS RIIS info to GBIF occurrences."""
import os

from bison.common.constants import (
    AGGREGATOR_DELIMITER, ENCODING, GBIF, NEW_RESOLVED_COUNTY, NEW_RESOLVED_STATE, RIIS_SPECIES, US_STATES,
    SPECIES_KEY, ASSESS_KEY, LOCATION_KEY, COUNT_KEY, LMBISON_HEADER)
from bison.common.riis import NNSL

from bison.tools.util import (get_csv_writer, get_csv_dict_reader, get_logger)

# For each temporary summary file, 3 fields, for each location: location, species, count
# Summary files are used to individually process manageably-sized files, then read and aggregate summaries for final output
# SUMMARY_HEADER = [LOCATION_KEY, SPECIES_KEY, COUNT_KEY]
#
# # Data Product 1: For each state or county file, 3 fields, for each species: species, count, assessment
# REGION_FILE_HEADER = [SPECIES_KEY, COUNT_KEY, ASSESS_KEY]
# # Data Product 2: For each state and county, 1 record with counts and percents of
# #                 introduced, invasive, presumed_native occurrences and species
# GBIF_RIIS_SUMMARY_HEADER = [
#     STATE_KEY, COUNTY_KEY,
#     "introduced_species", "invasive_species",  "presumed_native_species", "all_species",
#     "pct_introduced_all_species", "pct_invasive_all_species", "pct_presumed_native_species",
#     "introduced_occurrences", "invasive_occurrences", "presumed_native_occurrences", "all_occurrences",
#     "pct_introduced_all_occurrences", "pct_invasive_all_occurrences", "pct_presumed_native_occurrences"]


# .............................................................................
class RIIS_Counts():
    """Class for assembling counts for a RIIS assessment.

    Goal:
        All US occurrences will be assessed by the USGS RIIS as introduced, invasive, or presumed_native. This class
            contains counts for either occurrences or species, and asserts that the counts are consistent
            (i.e. introduced + invasive + presumed_native = total
            and %_introduced + %_invasive + %_presumed_native = 1.0)
    """
    def __init__(self, introduced=0, invasive=0, presumed_native=0, is_group=False, logger=None):
        """Constructor.

        Note:
            Counts are cast as integers.

        Args:
            introduced (int): Count of introduced group or occurrences
            invasive (int):  Count of introduced group or occurrences
            presumed_native (int): Count of presumed_native group or occurrences
            is_group (bool): True if counts are for a group, such as species, or individual occurrences.
            logger (object): logger for saving relevant processing messages
        """
        self.introduced = int(introduced)
        self.invasive = int(invasive)
        self.presumed_native = int(presumed_native)
        self.is_group = is_group
        self._log = logger

    # .............................................................................
    def add(self, other):
        """Add the values in another RIIS_Counts object to values in this object.

        Args:
            other (RIIS_Counts): another object for which to add values to self

        Raises:
            Exception: on attempt to add different types of counts together
        """
        if self.is_group is other.is_group:
            self.introduced += other.introduced
            self.invasive += other.invasive
            self.presumed_native += other.presumed_native
        else:
            raise Exception("Cannot add group (species) counts to individual occurrence counts")

    # .............................................................................
    def add_to(self, assessment, value=1):
        """Add the values in another RIIS_Counts object to values in this object.

        Args:
            other (RIIS_Counts): another object for which to add values to self

        Raises:
            Exception: on attempt to add different types of counts together
        """
        if assessment == "introduced":
            self.introduced += int(value)
        elif assessment == "invasive":
            self.invasive += int(value)
        elif assessment == "presumed_native":
            self.presumed_native += int(value)
        else:
            raise Exception(f"Cannot add to invalid assessment {assessment}")

    # .............................................................................
    def equals(self, other):
        """Compare the values in another RIIS_Counts object to values in this object.

        Args:
            other (RIIS_Counts): another object for which to add values to self

        Returns:
            boolean indicating whether self and other attribute values are equivalent.

        Raises:
            Exception: on attempt to add different types of counts together
        """
        if self.is_group is other.is_group:
            if self.introduced != other.introduced:
                self._log.info(f"Introduced count: self {self.introduced} <> other {other.introduced}")
                return False
            if self.invasive != other.invasive:
                self._log.info(f"Invasive count: self {self.introduced} <> other {other.introduced}")
                return False
            if self.presumed_native != other.presumed_native:
                self._log.info(f"Presumed_native count: self {self.presumed_native} <> other {other.presumed_native}")
                return False
        else:
            raise Exception("Cannot add group (species) counts to individual occurrence counts")
        return True

    # .............................................................................
    @property
    def total(self):
        """Return the total introduced, invasive, and presumed native counts.

        Returns:
            total of all assessment counts
        """
        return self.introduced + self.invasive + self.presumed_native

    # .............................................................................
    @property
    def percent_introduced(self):
        """Compute the percent introduced of total count.

        Returns:
            percentage of introduced count to total count
        """
        return (self.introduced / self.total)

    # .............................................................................
    @property
    def percent_introduced_rnd(self):
        """The rounded percent introduced of total count, as a string.

        Returns:
            string of the percentage of introduced count to total count, rounded to 3 places right of the decimal
        """
        return f"{self.percent_introduced:.3f}"

    # .............................................................................
    @property
    def percent_invasive(self):
        """Compute the percent invasive of total count.

        Returns:
            percentage of invasive count to total count
        """
        return (self.invasive / self.total)

    # .............................................................................
    @property
    def percent_invasive_rnd(self):
        """The rounded percent invasive of total count, as a string.

        Returns:
            string of the percentage of invasive count to total count, rounded to 3 places right of the decimal
        """
        return f"{self.percent_invasive:.3f}"

    # .............................................................................
    @property
    def percent_presumed_native(self):
        """Compute the percent presumed_native of total count.

        Returns:
            percentage of presumed_native count to total count
        """
        return (self.presumed_native / self.total)

    # .............................................................................
    @property
    def percent_presumed_native_rnd(self):
        """The rounded percent presumed_native of total count, as a string.

        Returns:
            string of the percentage of presumed_native count to total count, rounded to 3 places right of the decimal
        """
        return f"{self.percent_presumed_native:.3f}"


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
    @classmethod
    def construct_compound_key(cls, part1, part2):
        """Construct a compound key for dictionaries.

        Args:
            part1 (str): first element of compound key.
            part2 (str): second element of compound key.

        Returns:
             str combining part1 and part2 to use as a dictionary key.
        """
        return f"{part1}{AGGREGATOR_DELIMITER}{part2}"

    # ...............................................
    @classmethod
    def parse_compound_key(cls, compound_key):
        """Parse a compound key into its elements.

        Args:
             compound_key (str): key combining 2 elements.

        Returns:
            part1 (str): first element of compound key.
            second (str): first element of compound key.
        """
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
    def construct_assessment_summary_name(cls, datapath):
        """Construct a filename for the RIIS assessment summary file.

        Args:
            datapath (str): full directory path for computations and output.

        Returns:
            outfname: output filename
        """
        outfname = os.path.join(datapath, "out", "riis_summary.csv")
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
        county = None
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
                species_key = self.construct_compound_key(rec[GBIF.ACC_TAXON_FLD], rec[GBIF.ACC_NAME_FLD])
                state = rec[NEW_RESOLVED_STATE]
                county = rec[NEW_RESOLVED_COUNTY]
                county_state = self.construct_compound_key(state, county)

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
            csv_wtr.writerow(LMBISON_HEADER.SUMMARY_FILE)
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
            summary_filename = self.construct_location_summary_name(self._datapath, state)
        else:
            location = self.construct_compound_key(state, county)
            summary_filename = self.construct_location_summary_name(self._datapath, state, county=county)

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
                csv_wtr.writerow(LMBISON_HEADER.REGION_FILE)
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

        sp_counts = RIIS_Counts(is_group=True)
        occ_counts = RIIS_Counts(is_group=False)

        # Count introduced/invasive
        for _species, count in assess["introduced"].items():
            sp_counts.introduced += 1
            occ_counts.introduced += int(count)
        for _species, count in assess["invasive"].items():
            sp_counts.invasive += 1
            occ_counts.invasive += int(count)
        for _species, count in assess["presumed_native"].items():
            sp_counts.presumed_native += 1
            occ_counts.presumed_native += int(count)

        pct_sp = (sp_counts.percent_introduced + sp_counts.percent_invasive + sp_counts.percent_presumed_native)
        if (sp_counts.percent_introduced + sp_counts.percent_invasive + sp_counts.percent_presumed_native) != 1:
            self._log.info(f"Percent species totals for {state} {county}: {pct_sp} <> 1.0")

        pct_occ = (occ_counts.percent_introduced + occ_counts.percent_invasive + occ_counts.percent_presumed_native)
        if pct_occ != 1:
            self._log.info(f"Percent occ totals for {state} {county}: {pct_occ} <> 1.0")
        """
        STATE_KEY, COUNTY_KEY,
        "introduced_species", "invasive_species",  "presumed_native_species", "all_species",
        "pct_introduced_all_species", "pct_invasive_all_species", "pct_presumed_native_species",
        "introduced_occurrences", "invasive_occurrences", "presumed_native_occurrences", "all_occurrences",
        "pct_introduced_all_occurrences", "pct_invasive_all_occurrences", "pct_presumed_native_occurrences"]
        """
        csvwriter.writerow([
            state, county,
            sp_counts.introduced, sp_counts.invasive, sp_counts.presumed_native, sp_counts.total,
            sp_counts.percent_introduced_rnd, sp_counts.percent_invasive_rnd, sp_counts.percent_presumed_native_rnd,
            occ_counts.introduced, occ_counts.invasive, occ_counts.presumed_native, occ_counts.total,
            occ_counts.percent_introduced_rnd, occ_counts.percent_invasive_rnd, occ_counts.percent_presumed_native_rnd,
        ])

        return sp_counts, occ_counts

    # ...............................................
    def summarize_regions(self, summary_filename_list):
        """Read annotated data from one or more files, summarize by species, and by location.

        Args:
            summary_filename_list (list): list of full summary input filenames to be read.

        Returns:
            region_summary_filenames (list): full region filenames of counts and percentages for each species.
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
            region_summary_filenames (list): full region filenames of counts and percentages for each species.

        Returns:
            assess_summary_filename (str): full filename of introduced, invasive, presumed_native counts and percentages
                for each region.

        Raises:
            Exception: on unexpected open or write error
        """
        self.locations = {}
        assess_summary_filename = self.construct_assessment_summary_name(self._datapath)

        try:
            csvwtr, outf = get_csv_writer(assess_summary_filename, GBIF.DWCA_DELIMITER, fmode="w", overwrite=True)
            csvwtr.writerow(LMBISON_HEADER.GBIF_RIIS_SUMMARY_FILE)
        except Exception as e:
            raise Exception(f"Unknown open/csv error on {assess_summary_filename}: {e}")

        region_species_counts = RIIS_Counts(is_group=True)
        region_occ_counts = RIIS_Counts(is_group=False)
        sub_region_species_counts = RIIS_Counts(is_group=True)
        sub_region_occ_counts = RIIS_Counts(is_group=False)

        try:
            # Summarize introduced/invasive/native in each location
            for region_file in region_summary_filenames:
                super_region, sub_region = self.parse_location_summary_name(region_file)
                sp_counts, occ_counts = self._summarize_region_by_riis(
                    csvwtr, region_file, super_region, county=sub_region)
                if sub_region is None:
                    region_species_counts.add(sp_counts)
                    region_occ_counts.add(occ_counts)
                else:
                    sub_region_species_counts.add(sp_counts)
                    sub_region_occ_counts.add(occ_counts)

            self._log.info(f"Region totals {region_species_counts.total} ?= subregion {sub_region_species_counts.total}")
            self._log.info(f"Assessed occurrence totals {region_occ_counts.total} ?= subregion {sub_region_occ_counts.total}")

            self._log.info(f"Wrote RIIS assessment summaries to {assess_summary_filename}")
        except Exception as e:
            raise Exception(f"Unknown write error on {assess_summary_filename}: {e}")
        finally:
            outf.close()

        return assess_summary_filename
