"""Common classes for adding USGS RIIS info to GBIF occurrences."""
import csv
from datetime import datetime
from multiprocessing import Pool, cpu_count
import os

from bison.common.constants import (
    AGGREGATOR_DELIMITER, ASSESS_KEY, COUNT_KEY, DATA_PATH, ENCODING, EXTRA_CSV_FIELD,
    GBIF, LMBISON_HEADER, LOCATION_KEY, NEW_RESOLVED_COUNTY, NEW_RESOLVED_STATE,
    OUT_DIR, RIIS, SPECIES_NAME_KEY, SPECIES_KEY, US_STATES)
from bison.common.log import Logger
from bison.common.util import (get_csv_writer, get_csv_dict_reader, ready_filename)

from bison.providers.riis_data import NNSL


# .............................................................................
class RIIS_Counts():
    """Class for assembling counts for a RIIS assessment.

    Goal:
        All US occurrences will be assessed by the USGS RIIS as introduced, invasive,
        or presumed_native. This class contains counts for either occurrences or
        species, and asserts that the counts are consistent
        (i.e. introduced + invasive + presumed_native = total AND
              %_introduced + %_invasive + %_presumed_native = 1.0)
    """
    def __init__(
            self, logger, introduced=0, invasive=0, presumed_native=0, is_group=False):
        """Constructor.

        Note:
            Counts are cast as integers.

        Args:
            logger (object): logger for saving relevant processing messages
            introduced (int): Count of introduced group or occurrences
            invasive (int):  Count of introduced group or occurrences
            presumed_native (int): Count of presumed_native group or occurrences
            is_group (bool): True if counts are for a group, such as species, or
                individual occurrences.
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
            raise Exception(
                "Cannot add group (species) counts to individual occurrence counts")

    # .............................................................................
    def add_to(self, assessment, value=1):
        """Add the values in another RIIS_Counts object to values in this object.

        Args:
            assessment (str): type of assessment for which to add values
            value (int): count to add to the appropriate assessment

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
                return False
            if self.invasive != other.invasive:
                return False
            if self.presumed_native != other.presumed_native:
                return False
        else:
            raise Exception(
                "Cannot add group (species) counts to individual occurrence counts")
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
            percentage of introduced count to total count or "n/a" if total is 0.
        """
        try:
            pct = (self.introduced / self.total)
        except ZeroDivisionError:
            return "n/a"
        return pct

    # .............................................................................
    @property
    def percent_introduced_rnd(self):
        """The rounded percent introduced of total count, as a string.

        Returns:
            string of the percentage of introduced count to total count, rounded to
                3 places right of the decimal
        """
        try:
            pct_str = f"{self.percent_introduced:.3f}"
        except SyntaxError:
            return self.percent_introduced
        return pct_str

    # .............................................................................
    @property
    def percent_invasive(self):
        """Compute the percent invasive of total count or "n/a" if total is 0.

        Returns:
            percentage of invasive count to total count or "n/a" if total is 0
        """
        try:
            pct = (self.invasive / self.total)
        except ZeroDivisionError:
            return "n/a"
        return pct

    # .............................................................................
    @property
    def percent_invasive_rnd(self):
        """The rounded percent invasive of total count, as a string.

        Returns:
            string of the percentage of invasive count to total count, rounded to
                3 places right of the decimal, or "n/a" if total is 0
        """
        try:
            pct_str = f"{self.percent_invasive:.3f}"
        except SyntaxError:
            return self.percent_invasive
        return pct_str

    # .............................................................................
    @property
    def percent_presumed_native(self):
        """Compute the percent presumed_native of total count.

        Returns:
            percentage of presumed_native count to total count or "n/a" if total is 0
        """
        try:
            pct = (self.presumed_native / self.total)
        except ZeroDivisionError:
            return "n/a"
        return pct

    # .............................................................................
    @property
    def percent_presumed_native_rnd(self):
        """The rounded percent presumed_native of total count, as a string.

        Returns:
            string of the percentage of presumed_native count to total count, rounded
                to 3 places right of the decimal or "n/a" if total is 0
        """
        try:
            pct_str = f"{self.percent_presumed_native:.3f}"
        except SyntaxError:
            return self.percent_presumed_native
        return pct_str


# .............................................................................
class Aggregator():
    """Class for summarizing GBIF data annotated with USGS RIIS assessments.

    Goal:
     Finally, we will summarize the results of data processing.
     * One set of results will include a list of “introduced” species and a list of
       “presumed native” species for each state and county, along with counts for
       each of these species.
     * The second set of results will show the summary counts of introduced species
       and all species, introduced occurrences and all occurrences, the percentage of
       introduced to all species, the percentage of introduced to all occurrences,
       all by state and county
    """
    def __init__(self, annotated_filename, logger):
        """Constructor.

        Args:
            annotated_filename (str): full filename for annotated occurrence CSV file
            logger (object): logger for saving relevant processing messages

        Raises:
            FileNotFoundError: on missing annotated_filename
        """
        if not os.path.exists(annotated_filename):
            raise FileNotFoundError(f"File {annotated_filename} does not exist")

        datapath, _ = os.path.split(annotated_filename)
        self._datapath = datapath
        self._csvfile = annotated_filename
        self._log = logger

        # Hold all counties found in each state
        self.states = {}
        self._init_states()

        # {county_or_state: {species: count, ... } ...  }
        self.locations = {}
        # {species_key: species_name,  ... }
        self._canonicals = {}

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

        Raises:
            ValueError: on unexpected input
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
            csvfile (str): full filename used to construct an annotated filename for
                this data.

        Returns:
            outfname: output filename derived from the annotated GBIF DWC filename
        """
        basename, ext = os.path.splitext(csvfile)
        outfname = f"{basename}_summary{ext}"
        return outfname

    # ...............................................
    @classmethod
    def construct_location_summary_name(cls, datapath, state, county=None):
        """Construct a filename for the summary file for a state and optional county.

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
        outfname = os.path.join(datapath, OUT_DIR, basename)
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
        outfname = os.path.join(datapath, OUT_DIR, "riis_summary.csv")
        return outfname

    # ...............................................
    @classmethod
    def parse_location_summary_name(cls, csvfile):
        """Construct a filename for the summarized version of csvfile.

        Args:
            csvfile (str): full filename used to construct an annotated filename
                for this data.

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
            raise Exception(
                f"Filename {csvfile} cannot be parsed into location elements")
        return state, county

    # ...............................................
    def _save_key_canonical(self, species_key, species_name):
        # canonicals = {species_key: species_name,  ... }
        try:
            existing_name = self._canonicals[species_key]
            if existing_name != species_name:
                bad_vals = f"{species_key}: {existing_name}, {species_name}"
                raise Exception(f"Multiple species values for species_key {bad_vals}")
        except KeyError:
            self._canonicals[species_key] = species_name

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

        # Add to summary of counties encountered in states
        try:
            state, county = self.parse_compound_key(location)
        except ValueError:
            raise
        else:
            if county is not None:
                try:
                    self.states[state].add(county)
                except KeyError:
                    self._log.error(f"Unexpected state {state} found")

    # ...............................................
    def _summarize_annotations_by_region(self):
        # Reset summary
        self.locations = {}
        csv_rdr, inf = get_csv_dict_reader(
            self._csvfile, GBIF.DWCA_DELIMITER, encoding=ENCODING, quote_none=True,
            restkey=EXTRA_CSV_FIELD)
        self._log.info(f"Summarizing annotations in {self._csvfile} by region")
        try:
            for rec in csv_rdr:
                state = rec[NEW_RESOLVED_STATE]
                county = rec[NEW_RESOLVED_COUNTY]
                # If record is filtered out for any reason during annotation,
                # state and county will be None
                if state and county:
                    species_key = self.construct_compound_key(
                        rec[GBIF.ACC_TAXON_FLD], rec[GBIF.ACC_NAME_FLD])
                    self._save_key_canonical(species_key, rec[GBIF.SPECIES_NAME_FLD])
                    county_state = self.construct_compound_key(state, county)
                    self._add_record_to_location_summaries(county_state, species_key)
                    self._add_record_to_location_summaries(state, species_key)

        except csv.Error as ce:
            self._log.error(
                f"Failed to read annotated occurrences line {csv_rdr.line_num} from \
                {self._csvfile}: {ce}")
        except Exception as e:
            self._log.error(
                f"Exception {type(e)}: Failed to read annotated occurrences line \
                {csv_rdr.line_num} from {self._csvfile}: {e}")

        finally:
            csv_rdr = None
            inf.close()

    # ...............................................
    def _write_region_summary(self, summary_filename, overwrite=True):
        # header = [LOCATION_KEY, SPECIES_KEY, SPECIES_NAME, COUNT_KEY]
        ready_filename(summary_filename, overwrite=overwrite)
        try:
            csv_wtr, outf = get_csv_writer(
                summary_filename, GBIF.DWCA_DELIMITER, fmode="w")
        except Exception as e:
            raise Exception(f"Failed to open summary CSV file {summary_filename}: {e}")

        try:
            csv_wtr.writerow(LMBISON_HEADER.SUMMARY_FILE)
            self._log.info(f"Writing region summaries to {summary_filename}")

            # Location keys are state and county_state
            for location, sp_info in self.locations.items():
                for species_key, count in sp_info.items():
                    species_name = self._canonicals[species_key]
                    row = [location, species_key, species_name, count]
                    csv_wtr.writerow(row)
        except Exception as e:
            raise Exception(f"Failed to write to file {summary_filename}, ({e})")
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
            except Exception as e:
                raise Exception(f"Failed to open {sum_fname}: {e}")

            try:
                for rec in csv_rdr:
                    species_key = rec[SPECIES_KEY]
                    self._add_record_to_location_summaries(
                        rec[LOCATION_KEY], species_key, count=rec[COUNT_KEY])
                    try:
                        self._canonicals[species_key]
                    except KeyError:
                        self._canonicals[species_key] = rec[SPECIES_NAME_KEY]
            except Exception as e:
                raise Exception(f"Failed to read {sum_fname}: {e}")
            finally:
                inf.close()

    # ...............................................
    def summarize_by_file(self, overwrite=True):
        """Read an annotated file, summarize by species and location, write to csvfile.

        Args:
            overwrite (bool): Flag indicating whether to overwrite existing files.

        Returns:
            summary_filename: full output filename for CSV summary of annotated
                occurrence file, with fields:
            SPECIES_KEY, GBIF_TAXON_KEY, ASSESS_KEY, STATE_KEY, COUNTY_KEY, COUNT_KEY
        """
        summary_filename = self.construct_summary_name(self._csvfile)
        # Summarize and write
        self._summarize_annotations_by_region()
        self._write_region_summary(summary_filename, overwrite=overwrite)
        self._log.info(
            f"Summarized species by region from {self._csvfile} to {summary_filename}")
        return summary_filename

    # ...............................................
    def _get_riis_species(self):
        riis_filename = os.path.join(DATA_PATH, RIIS.SPECIES_GEO_FNAME)
        nnsl = NNSL(riis_filename, logger=self._log)
        nnsl.read_riis()
        return nnsl

    # ...............................................
    def _examine_species_for_location(self, region, species_counts, nnsl):
        recs = []
        if species_counts:
            # All these species are for the location of interest
            for species_key, count in species_counts.items():
                try:
                    gbif_taxon_key, scientific_name = self.parse_compound_key(
                        species_key)
                except ValueError:
                    raise (
                        f"Error in species_key {species_key}, should contain name "
                        "and taxonKey")

                try:
                    species_name = self._canonicals[species_key]
                except KeyError:
                    raise (f"Missing species name for key {species_key}")

                assessments = nnsl.get_assessments_for_gbif_taxonkey(gbif_taxon_key)
                try:
                    assess = assessments[region].lower()
                except KeyError:
                    assess = "presumed_native"
                # Record contents: LMBISON_HEADER.REGION_FILE
                recs.append([species_key, scientific_name, species_name, count, assess])
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

        [species_key, species_name, count, assessment]
        """
        if county is None:
            location = state
            summary_filename = self.construct_location_summary_name(
                self._datapath, state)
        else:
            location = self.construct_compound_key(state, county)
            summary_filename = self.construct_location_summary_name(
                self._datapath, state, county=county)

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
                csv_wtr, outf = get_csv_writer(
                    summary_filename, GBIF.DWCA_DELIMITER, fmode="w")
            except Exception as e:
                raise Exception(f"Unknown write error on {summary_filename}: {e}")

            try:
                csv_wtr.writerow(LMBISON_HEADER.REGION_FILE)
                # Write all records found
                records = self._examine_species_for_location(
                    region, species_counts, nnsl)
                for rec in records:
                    csv_wtr.writerow(rec)
                self._log.info(f"Wrote region summaries to {summary_filename}")
            except Exception as e:
                raise Exception(f"Unknown write error on {summary_filename}: {e}")
            finally:
                outf.close()

        return summary_filename

    # ...............................................
    def _summarize_region_by_riis(
            self, csvwriter, loc_summary_file, state, county=None):
        # read [SPECIES_KEY, COUNT_KEY, ASSESS_KEY]
        assess = {"introduced": {}, "invasive": {}, "presumed_native": {}}
        try:
            rdr, inf = get_csv_dict_reader(
                loc_summary_file, GBIF.DWCA_DELIMITER, encoding=ENCODING,
                quote_none=False)
        except Exception as e:
            raise Exception(f"Unknown open error on {loc_summary_file}: {e}")

        try:
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

        sp_counts = RIIS_Counts(self._log, is_group=True)
        occ_counts = RIIS_Counts(self._log, is_group=False)

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

        pct_sp = (
                sp_counts.percent_introduced
                + sp_counts.percent_invasive
                + sp_counts.percent_presumed_native)
        if pct_sp != 1:
            self._log.info(
                f"Percent species totals for {state} {county}: {pct_sp} <> 1.0")

        pct_occ = (
                occ_counts.percent_introduced
                + occ_counts.percent_invasive
                + occ_counts.percent_presumed_native)
        if pct_occ != 1:
            self._log.info(f"Percent occ totals for {state} {county}: {pct_occ} <> 1.0")

        csvwriter.writerow([
            state, county,
            sp_counts.introduced, sp_counts.invasive, sp_counts.presumed_native,
            sp_counts.total,
            sp_counts.percent_introduced_rnd, sp_counts.percent_invasive_rnd,
            sp_counts.percent_presumed_native_rnd,
            occ_counts.introduced, occ_counts.invasive, occ_counts.presumed_native,
            occ_counts.total,
            occ_counts.percent_introduced_rnd, occ_counts.percent_invasive_rnd,
            occ_counts.percent_presumed_native_rnd,
        ])

        return sp_counts, occ_counts

    # ...............................................
    def aggregate_regions(self, summary_filename_list):
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
                cty_agg_fname = self._write_region_aggregate(
                    datapath, nnsl, state, county=county)
                if cty_agg_fname is not None:
                    region_summary_filenames.append(cty_agg_fname)

        return region_summary_filenames

    # ...............................................
    def aggregate_assessments(self, region_summary_filenames):
        """Read annotated data from >= 1 files, summarize by species and location.

        Args:
            region_summary_filenames (list): full region filenames of counts and
                percentages for each species.

        Returns:
            assess_summary_filename (str): full filename of introduced, invasive,
                presumed_native counts and percentages for each region.

        Raises:
            Exception: on unexpected open or write error
        """
        self.locations = {}
        assess_summary_filename = self.construct_assessment_summary_name(self._datapath)

        try:
            csvwtr, outf = get_csv_writer(
                assess_summary_filename, GBIF.DWCA_DELIMITER, fmode="w", overwrite=True)
            csvwtr.writerow(LMBISON_HEADER.GBIF_RIIS_SUMMARY_FILE)
        except Exception as e:
            raise Exception(f"Unknown open/csv error on {assess_summary_filename}: {e}")

        region_species_counts = RIIS_Counts(self._log, is_group=True)
        region_occ_counts = RIIS_Counts(self._log, is_group=False)
        sub_region_species_counts = RIIS_Counts(self._log, is_group=True)
        sub_region_occ_counts = RIIS_Counts(self._log, is_group=False)

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

            self._log.info(
                f"Region totals {region_species_counts.total} ?= \
                 subregion {sub_region_species_counts.total}")
            self._log.info(
                f"Assessed occurrence totals {region_occ_counts.total} ?= \
                subregion {sub_region_occ_counts.total}")

            self._log.info(
                f"Wrote RIIS assessment summaries to {assess_summary_filename}")
        except Exception as e:
            raise Exception(f"Unknown write error on {assess_summary_filename}: {e}")
        finally:
            outf.close()

        return assess_summary_filename


# .............................................................................
def summarize_annotations(ann_filename, log_directory):
    """Summarize data in an annotated GBIF DwC file by state, county, and RIIS.

    Args:
        ann_filename (str): full filename of an annotated GBIF data file.
        log_directory (str): destination directory for logfile

    Returns:
        summary_filename (str): full filename of a summary file

    Raises:
        FileNotFoundError: on missing input file
    """
    if not os.path.exists(ann_filename):
        raise FileNotFoundError(ann_filename)

    datapath, basefname = os.path.split(ann_filename)
    refname = f"summarize_{basefname}"
    logger = Logger(refname, os.path.join(log_directory, f"{refname}.log"))
    logger.log(f"Submit {basefname} for summarizing.", refname=refname)

    logger.log(f"Start Time : {datetime.now()}", refname=refname)
    agg = Aggregator(ann_filename, logger)
    # Do not overwrite existing summary
    summary_filename = agg.summarize_by_file(overwrite=False)
    logger.log(f"End Time : {datetime.now()}", refname=refname)
    return summary_filename


# .............................................................................
def parallel_summarize(annotated_filenames, main_logger):
    """Main method for parallel execution of summarization script.

    Args:
        annotated_filenames (list): list of full filenames containing annotated
            GBIF data.
        main_logger (logger): logger for the process that calls this function,
            initiating subprocesses

    Returns:
        annotated_dwc_fnames (list): list of full output filenames
    """
    refname = "parallel_summarize"
    inputs = []
    for in_csv in annotated_filenames:
        out_csv = Aggregator.construct_summary_name(in_csv)
        if os.path.exists(out_csv):
            main_logger.info(
                f"Summaries exist in {out_csv}, moving on.", refname=refname)
        else:
            inputs.append((in_csv, main_logger.log_directory))

    main_logger.info(
        f"Parallel Summarize Start Time : {datetime.now()}", refname=refname)
    # Do not use all CPUs
    pool = Pool(cpu_count() - 2)
    # Map input files asynchronously onto function
    map_result = pool.starmap_async(summarize_annotations, inputs)
    # Wait for results
    map_result.wait()
    summary_filenames = map_result.get()
    main_logger.log(f"Parallel Summarize End Time : {datetime.now()}", refname=refname)

    return summary_filenames