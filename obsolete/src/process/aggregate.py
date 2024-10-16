"""Common classes for adding USGS RIIS info to GBIF occurrences."""
import csv
import logging
import os
from datetime import datetime
import multiprocessing

from obsolete.src.common.constants2 import (
    APPEND_TO_DWC, LMBISON_PROCESS, ENCODING, EXTRA_CSV_FIELD, GBIF, LMBISON, REGION,
    REPORT, US_STATES)
from bison.common.log import Logger
from obsolete.src.common.util import (
    available_cpu_count, BisonKey, BisonNameOp, get_csv_dict_reader,
    get_csv_dict_writer, get_csv_writer, get_fields_from_header, ready_filename)
from bison.provider.gbif_data import DwcData
from bison.provider.riis_data import RIIS


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
        # self.counts = {}
        # for ass in LMBISON.assess_values():
        #     self.counts[ass] = {}
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
        except ValueError:
            pass
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
     * One set of results will include a list of “introduced”, a list of “invasive”
       and a list of “presumed native” species for each region (state, county,
       aiannh area, PAD area), along with counts for each of these species.
     * The second set of results will show the summary counts of introduced species
       and all species, introduced occurrences and all occurrences, the percentage of
       introduced to all species, the percentage of introduced to all occurrences,
       all by state and county
    """
    def __init__(self, logger):
        """Constructor.

        Args:
            logger (object): logger for saving relevant processing messages
        """
        self._log = logger
        # csv.DictReader and DictWriter
        self._csv_reader = None
        self._csv_writer = None
        # Input output file objects
        self._inf = None
        self._outf = None
        # Hold all counties found in each state
        self._states = {}
        self._init_states()

        # Reset summaries by location (self._locations)
        #   and species name lookup (self._acc_species_name)
        self._reset_summaries()

    # ...............................................
    def close(self):
        """Close input datafiles and output file."""
        self._csv_reader = None
        self._csv_writer = None
        try:
            self._inf.close()
        except AttributeError:
            pass
        try:
            self._outf.close()
        except AttributeError:
            pass

    # ...............................................
    @property
    def is_open(self):
        """Return true if input or output files are open.

        Returns:
            :type bool, True if a file is open, False if not
        """
        if ((self._inf is not None and not self._inf.closed) or
                (self._outf is not None and not self._outf.closed)):
            return True
        return False

    # ...............................................
    def _initialize_summary_io(
            self, annotated_filename, output_summary_filename, overwrite=True):
        """Initialize and open required input and output files of occurrence records.

        Also reads the first record and writes the header.

        Args:
            annotated_filename (str): full filename input annotated DwC occurrence file.
            output_summary_filename (str): destination full filename for the summary.
            overwrite (bool): True to overwrite existing file.

        Raises:
            FileNotFoundError: on missing input file.
            Exception: on missing output filename.
            Exception: on failure to open output file.
        """
        self.close()
        # Reset summaries by location and species name lookup
        self._reset_summaries()
        if not os.path.exists(annotated_filename):
            raise FileNotFoundError(f"File {annotated_filename} does not exist")
        if output_summary_filename is None:
            raise Exception(
                "Must provide an output filename to write summarized data")

        self._annotated_dwc_filename = annotated_filename

        self._csv_reader, self._inf = get_csv_dict_reader(
            self._annotated_dwc_filename, GBIF.DWCA_DELIMITER, encoding=ENCODING,
            quote_none=True, restkey=EXTRA_CSV_FIELD)

        if ready_filename(output_summary_filename, overwrite=overwrite):
            try:
                self._csv_writer, self._outf = get_csv_writer(
                    output_summary_filename, GBIF.DWCA_DELIMITER,
                    header=LMBISON.summary_header_temp(), fmode="w")
            except Exception as e:
                raise Exception(
                    f"Failed to open summary CSV file {output_summary_filename}: {e}")

    # ...............................................
    def _initialize_combine_summaries_io(self, combined_summary_filename):
        """Initialize, open, and write header of output file for summary of summaries.

        Args:
            combined_summary_filename (str): destination full filename for the summary.

        Raises:
            Exception: on failure to open output file.
        """
        # Close input and output IO
        self.close()
        # Reset summaries by location and species name lookup
        self._reset_summaries()

        if ready_filename(combined_summary_filename, overwrite=True):
            try:
                self._csv_writer, self._outf = get_csv_writer(
                    combined_summary_filename, GBIF.DWCA_DELIMITER,
                    header=LMBISON.summary_header_temp(), fmode="w")
            except Exception as e:
                raise Exception(
                    f"Failed to open summary CSV file {combined_summary_filename}: {e}")

    # ...............................................
    def _init_states(self):
        self._states = {}
        for st in US_STATES.values():
            self._states[st] = set()

    # ...............................................
    def _add_record_to_location_summaries(
            self, prefix, is_disjoint, location, species_key, count=1):
        # locations = {prefix: {location: {species_key: count,  ... },
        #                       location: {species_key: count,  ...},
        #                       ... },
        #              ...
        #              "filtered": {"na": {"na": count},
        #               ... }
        try:
            count = int(count)
        except ValueError:
            raise ValueError(f"Species count {count} must be an integer")

        # Point cannot be summarized for regions it does not intersect with
        if not location:
            if is_disjoint:
                # Point cannot be summarized for regions it does not intersect with
                return
            else:
                # Point must intersect with contiguous regions
                raise ValueError(
                    f"Record must have a value for contiguous region {prefix}")
        # Add to summary of unique locations
        try:
            # Is location present?
            self._locations[prefix][location]
            try:
                # Is species present in location?
                self._locations[prefix][location][species_key] += count
            except KeyError:
                # Missing species in location, add species to location
                self._locations[prefix][location][species_key] = count

        except KeyError:
            # Location not found, add location with species
            self._locations[prefix][location] = {species_key: count}

        # If this is a combo county+state, add to summary of counties encountered
        # in each state
        if prefix == "county":
            try:
                (state, county) = BisonKey.parse_compound_key(location)
            except ValueError:
                raise
            else:
                if county is not None:
                    try:
                        self._states[state].add(county)
                    except KeyError:
                        self._log.log(
                            f"Unexpected state {state} found",
                            refname=self.__class__.__name__, log_level=logging.ERROR)

    # ...............................................
    def _reset_summaries(self):
        # Reset species name lookup
        self._acc_species_name = {
            LMBISON.NOT_APPLICABLE: (LMBISON.NOT_APPLICABLE, LMBISON.NOT_APPLICABLE)}
        # Reset location summary, populate with geography type
        self._locations = {}
        for prefix in REGION.summary_fields().keys():
            self._locations[prefix] = {}

    # ...............................................
    @property
    def summarized(self):
        """Boolean flag indicating whether the data has been summarized to a dictionary.

        Returns:
            Boolean value, true if dictionary self._locations is filled
        """
        loc_sums = 0
        for prefix in REGION.summary_fields().keys():
            loc_sums += len(self._locations[prefix])
        if (loc_sums == 0 or len(self._acc_species_name) == 1):
            return False
        else:
            return True

    # ...............................................
    def summary(self):
        """Return a message summarizing the number of species names and locations.

        Returns:
            msg: string of information.
        """
        msg = "Read the following unique values:"
        msg += f"   {len(self._acc_species_name)} accepted species names"
        for rtype, rloc in self._locations.items():
            msg += f"   {len(rloc)} {rtype} locations"
        return msg

    # ...............................................
    def get_location_summary(self):
        """Return a report of locations, species and occurrences.

        Returns:
            report: dictionary of metadata about the data in memory.
        """
        report = {}
        for prefix in self._locations.keys():
            loc_count = sp_count = occ_count = 0
            for __loc, spcount_dict in self._locations[prefix].items():
                loc_count += 1
                for __sp, count in spcount_dict.items():
                    occ_count += count
                    sp_count += 1
            report[prefix] = {
                    REPORT.LOCATION: loc_count,
                    REPORT.SPECIES: sp_count,
                    REPORT.OCCURRENCE: occ_count
            }
        return report

    # ...............................................
    def get_species_counts_for_location(self, region_type, region_name):
        """Return a list of species, with the count for each, in a particular region.

        Args:
            region_type (str): Type of region (state, county, aiannh, pad ...)
            region_name (str): region name

        Returns:
            list of tuples containing a species key and occurrence count
        """
        return self._locations[region_type][region_name]

    # ...............................................
    def get_locations_for_region_type(self, region_type):
        """Return a list of regions in a type of region.

        Args:
            region_type (str): Type of region (state, county, aiannh, pad ...)

        Returns:
            list of region names.
        """
        return list(self._locations[region_type].keys())

    # ...............................................
    def get_species_from_key(self, rr_species_key):
        """Return the GBIF accepted species name, and the canonical name for a key.

        Args:
            rr_species_key (str): A compound key used for a species occurring in
                a particular RIIS region (AK, HI, L48)

        self._acc_species_name[rr_species_key] = (
                        rec[GBIF.ACC_NAME_FLD], rec[GBIF.SPECIES_NAME_FLD])

        Returns:
            tuple containing a GBIF accepted species name, and canonical name
        """
        return self._acc_species_name[rr_species_key]

    # ...............................................
    def get_species_lookup(self):
        """Return a dictionary of keys: species keys and values: accepted species names.

        Returns:
            report: dictionary of species keys and accepted names.
        """
        return self._acc_species_name

    # ...............................................
    def _summarize_annotations_by_region(self):
        # Reset summaries by location and species name lookup
        self._reset_summaries()
        region_disjoint = REGION.region_disjoint()

        self._log.log(
            f"Summarizing annotations in {self._annotated_dwc_filename} by region",
            refname=self.__class__.__name__)
        try:
            for rec in self._csv_reader:
                # Use combo key-name to track species
                riis_region = self._get_riis_region(rec[APPEND_TO_DWC.RESOLVED_ST])
                rr_species_key = BisonKey.get_compound_key(
                    riis_region, rec[GBIF.ACC_TAXON_FLD])
                # Save with accepted name and species name in case accepted name
                # is for sub-species
                self._acc_species_name[rr_species_key] = (
                    rec[GBIF.ACC_NAME_FLD], rec[GBIF.SPECIES_NAME_FLD])

                # regions to summarize
                for prefix, fld in REGION.summary_fields().items():
                    # if prefix != LMBISON.SUMMARY_FILTER_HEADING:
                    is_disjoint = region_disjoint[prefix]
                    if isinstance(fld, str):
                        location = rec[fld]
                    elif isinstance(fld, tuple) and len(fld) == 2:
                        location = BisonKey.get_compound_key(
                            rec[fld[0]], rec[fld[1]])
                    else:
                        raise Exception(f"Bad summary fields {fld}")

                    self._add_record_to_location_summaries(
                        prefix, is_disjoint, location, rr_species_key)

        except csv.Error as ce:
            self._log.log(
                f"Failed to read annotated occurrences line {self._csv_reader.line_num}"
                f" from {self._annotated_dwc_filename}: {ce}",
                refname=self.__class__.__name__, log_level=logging.ERROR)
        except Exception as e:
            self._log.log(
                f"Exception {type(e)}: Failed to read annotated occurrences line "
                f"{self._csv_reader.line_num} from {self._annotated_dwc_filename}: {e}",
                refname=self.__class__.__name__, log_level=logging.ERROR)

        finally:
            self._csv_reader = None
            self._inf.close()

        report = self.get_location_summary()
        report[REPORT.PROCESS] = LMBISON_PROCESS.SUMMARIZE["postfix"]
        return report

    # ...............................................
    def _write_raw_region_summary(self):
        # Write summary of annotated records,
        #       with LOCATION_PREFIX, LOCATION_KEY, SPECIES_KEY, SPECIES_NAME, COUNT_KEY
        try:
            self._log.log(
                f"Writing region summaries to {self._outf.name}",
                refname=self.__class__.__name__)

            # Location_type is state, county, aiannh, pad
            for location_type, loc_info in self._locations.items():
                # Location keys are values in each location_type
                for location, sp_info in loc_info.items():
                    for species_key, count in sp_info.items():
                        try:
                            accepted_name, species_name = self._acc_species_name[
                                species_key]
                        except KeyError:
                            self._log.log(f"No species {species_key} in canonicals.")
                        except Exception:
                            raise
                        row = [
                            location_type, location, species_key, accepted_name,
                            species_name, count]
                        self._csv_writer.writerow(row)
        except Exception as e:
            raise Exception(f"Failed to write to file {self._outf.name}, ({e})")
        finally:
            self._outf.close()

    # ...............................................
    def summarize_assessments_by_region(self):
        """Summarize annotated records by RIIS assessments by region.

        Raises:
            Exception: on failure to write file.
        """
        try:
            self._log.log(
                "Summarizing region summaries to ",
                refname=self.__class__.__name__)

            # Location_type is state, county, aiannh, pad
            for location_type, loc_info in self._locations.items():
                # Location keys are values in each location_type
                for location, sp_info in loc_info.items():
                    for species_key, count in sp_info.items():
                        try:
                            accepted_name, species_name = self._acc_species_name[
                                species_key]
                        except KeyError:
                            self._log.log(f"No species {species_key} in canonicals.")
                        except Exception:
                            raise
                        row = [
                            location_type, location, species_key, accepted_name,
                            species_name, count]
                        self._csv_writer.writerow(row)
        except Exception as e:
            raise Exception(f"Failed to write to file {self._outf.name}, ({e})")
        finally:
            self._outf.close()

    # ...............................................
    def _read_location_summary(self, summary_filename):
        """Read summary files and combine summaries in self._locations.

        Args:
            summary_filename: full filename of raw summary by region.

        Raises:
            Exception: on failure to open or read a file.
        """
        try:
            csv_rdr, inf = get_csv_dict_reader(summary_filename, GBIF.DWCA_DELIMITER)
        except Exception as e:
            raise Exception(f"Failed to open {summary_filename}: {e}")

        try:
            for rec in csv_rdr:
                rrspkey = rec[LMBISON.RR_SPECIES_KEY]
                # is_disjoint = False to bypass for previously summarized data
                self._add_record_to_location_summaries(
                    rec[LMBISON.LOCATION_TYPE_KEY], False, rec[LMBISON.LOCATION_KEY],
                    rec[LMBISON.RR_SPECIES_KEY], count=rec[LMBISON.COUNT_KEY])

                # Fill species name lookup
                try:
                    _ = self._acc_species_name[rrspkey]
                except KeyError:
                    self._acc_species_name[rrspkey] = (
                        rec[LMBISON.SCIENTIFIC_NAME_KEY], rec[LMBISON.SPECIES_NAME_KEY])

        except Exception as e:
            raise Exception(f"Failed to read {summary_filename}: {e}")
        finally:
            inf.close()

    # ...............................................
    def _read_location_summaries(self, summary_filename_list):
        """Read summary files and combine summaries in self._locations.

        Args:
            summary_filename_list (list): full filenames of raw summary by region.

        Returns:
            report: dictionary of metadata about the data and process
        """
        # Reset location summary and species name lookup
        self._reset_summaries()
        # Add summaries from each summary
        for sum_fname in summary_filename_list:
            self._read_location_summary(sum_fname)

        summary_report = self._report_summary()
        return summary_report

    # ...............................................
    def _report_summary(self):
        summary = {}
        # Report count of locations for each region type
        for region_type, loc_dict in self._locations.items():
            # Count unique locations for each region type (county, state, PAD, AIANNH)
            summary[region_type] = {
                REPORT.LOCATION: len(loc_dict),
                REPORT.SPECIES: {},
                REPORT.OCCURRENCE: {}
            }
            for loc, sp_count in loc_dict.items():
                # Count occurrences
                occ_total = 0
                for occ_count in sp_count.values():
                    occ_total += occ_count
                # Record number of unique species
                summary[region_type][loc] = {
                    REPORT.SPECIES: len(sp_count),
                    REPORT.OCCURRENCE: occ_total
                }
        summary[REPORT.SPECIES] = len(self._acc_species_name)
        return summary

    # ...............................................
    def summarize_summaries(self, summary_filename_list, full_summary_filename):
        """Read summary files and combine summaries in self._locations.

        Args:
            summary_filename_list: list of full filenames of summary files.
            full_summary_filename (str): Full filename for combined output summary file.

        Returns:
            report: dictionary of metadata about the data and process
        """
        self._initialize_combine_summaries_io(full_summary_filename)
        summary_report = self._read_location_summaries(summary_filename_list)
        # Save to single file
        self._write_raw_region_summary()
        report = {
            REPORT.PROCESS: LMBISON_PROCESS.SUMMARIZE["postfix"],
            REPORT.INFILE: summary_filename_list,
            REPORT.OUTFILE: full_summary_filename,
            REPORT.SUMMARY: summary_report
        }

        self.close()
        return report

    # ...............................................
    def summarize_annotated_recs_by_location(
            self, annotated_filename, process_path, overwrite=True):
        """Read an annotated file, summarize by species and location, write to csvfile.

        Args:
            annotated_filename (str): full filename input annotated DwC occurrence file.
            process_path (str): destination directory for the unorganized summary by
                locations.
            overwrite (bool): Flag indicating whether to overwrite existing files.

        Returns:
            report (dict): Summary of the number of locations, species, and occurrences
                for each type of region for summary (state, county, aiannh, PAD).

        Note:
            summary file contains records like:
                SPECIES_KEY, GBIF_TAXON_KEY, ASSESS_KEY, STATE_KEY, COUNTY_KEY, COUNT_KEY
        """
        summary_filename = BisonNameOp.get_process_outfilename(
            annotated_filename, outpath=process_path,
            step_or_process=LMBISON_PROCESS.SUMMARIZE)

        self._initialize_summary_io(
            annotated_filename, summary_filename, overwrite=overwrite)
        # Summarize and write
        file_report = self._summarize_annotations_by_region()
        report = {
            REPORT.INFILE: annotated_filename,
            REPORT.OUTFILE: summary_filename,
            REPORT.SUMMARY: file_report
        }
        self._write_raw_region_summary()

        self._log.log(
            f"Summarized species by all regions from {annotated_filename} to "
            f"{summary_filename}", refname=self.__class__.__name__)
        return report

    # ...............................................
    def _get_outwriter(self, annotated_filename, value, overwrite):
        recsbyval_filename = BisonNameOp.get_recsbyval_outfilename(
            annotated_filename, value)

        header = get_fields_from_header(annotated_filename)
        try:
            csv_writer, outf = get_csv_dict_writer(
                recsbyval_filename, header, GBIF.DWCA_DELIMITER, fmode="w",
                encoding=ENCODING, overwrite=overwrite)
        except Exception as e:
            raise Exception(
                f"Failed to open file or csv_writer for {recsbyval_filename}, {e}")
        return csv_writer, outf, recsbyval_filename

    # ...............................................
    def aggregate_files_by_fieldval(
            self, annotated_filenames, fieldname, overwrite=True):
        """Read an annotated file, group records by value in csvfiles.

        Args:
            annotated_filenames (list): list of full filename input annotated DwC
                occurrence file.
            fieldname (str): fieldname for value used for aggregation.
            overwrite (bool): Flag indicating whether to overwrite existing files.

        Returns:
            report (dict): Summary of the number of locations, species, and occurrences
                for each type of region for summary (state, county, aiannh, PAD).

        Raises:
            Exception: on failure to open DwcData reader.

        Note:
            used for aggregating by a value, for streamlining additional annotation
            based on that value.
        """
        # Output files {ST: (csvwriter, outfile, outfilename)}
        outfiles = {}

        try:
            for ann_filename in annotated_filenames:
                # Input reader
                dwcdata = DwcData(ann_filename, logger=self._log)
                try:
                    dwcdata.open()
                except Exception:
                    raise
                # Read, annotate, write
                try:
                    written, total = self._aggregate_annotated_recs_by_fieldval(
                        dwcdata, fieldname, outfiles, overwrite=True)
                except Exception:
                    raise
                finally:
                    # Close input
                    dwcdata.close()

                # Summarize and write
                self._log.log(
                    f"Aggregated {written} of {total} records from {ann_filename} by value "
                    f"from {fieldname}", refname=self.__class__.__name__)
        finally:
            outfilenames = []
            for _, out_file, out_fn in outfiles.values():
                outfilenames.append(out_fn)
                # Close output
                out_file.close()

        return outfilenames

    # ...............................................
    def _aggregate_annotated_recs_by_fieldval(
            self, dwcdata, fieldname, outfiles, overwrite=True):
        """Read an annotated file, group records by value in csvfiles.

        Args:
            dwcdata (bison.provider.gbif_data.DwcData):  input reader for annotated DwC
                occurrence records.
            fieldname (str): fieldname for value used for aggregation.
            outfiles (dict): dictionary of keys == value of interest,
                values == tuple of (csv_dict_writer, out_file, out_filename)
            overwrite (bool): Flag indicating whether to overwrite existing files.

        Returns:
            written (int): Number of records successfully written to aggregated files.
            total (int): Number of records read from file.

        Raises:
            Exception: on unforeseen error in loop.

        Note:
            used for aggregating by a value, for streamlining additional annotation
            based on that value.
        """
        total = 0
        written = 0
        try:
            # iterate over DwC records
            dwcrec = dwcdata.get_record()

            # Only append additional values to records that pass the filter tests.
            while dwcrec is not None:
                total += 1
                val = dwcrec[fieldname]
                try:
                    # Find correct csvwriter
                    (csv_dict_writer, _, out_filename) = outfiles[val]
                except KeyError:
                    # Or create new csvwriter
                    csv_dict_writer, out_file, out_filename = self._get_outwriter(
                        dwcdata.input_file, val, overwrite)
                    outfiles[val] = (csv_dict_writer, out_file, out_filename)

                try:
                    csv_dict_writer.writerow(dwcrec)
                    written += 1
                except Exception as e:
                    self._log.log(
                        f"Error {e} record, gbifID {dwcrec[GBIF.ID_FLD]}, value {val}",
                        refname=self.__class__.__name__, log_level=logging.ERROR)
                # Get next
                dwcrec = dwcdata.get_record()

        except Exception as e:
            raise Exception(
                f"Unexpected error {e} reading {dwcdata.input_file}, line "
                f"{dwcdata.recno} or writing.")

        return written, total

    # ...............................................
    def _write_region_aggregate(
            self, riis, region_type, region_value, outpath, overwrite=True):
        """Summarize aggregated data for a location by species.

        Args:
            riis (bison.common.riis.RIIS): Non-native species list object
            region_type (str): Type of region for file prefix (state, county, ...)
            region_value (str): region name
            outpath (str): output path for aggregate file
            overwrite (bool): Flag indicating whether to overwrite existing output files.

        Returns:
            summary_filename: output file

        Raises:
            Exception: on unexpected open file error
            Exception: on missing rr_species_key in _acc_species_name lookup
            Exception: on unexpected write error
            Exception: on unexpected error processing record

        [species_key, species_name, count, assessment]
        """
        agg_fname = BisonNameOp.get_location_summary_name(
            outpath, region_type, region_value)

        try:
            csv_wtr, outf = get_csv_writer(
                agg_fname, GBIF.DWCA_DELIMITER,
                header=LMBISON.region_summary_header(), fmode="w", overwrite=overwrite)
        except Exception as e:
            raise Exception(f"Unknown write error on {agg_fname}: {e}")

        rr_species_counts = self._locations[region_type][region_value]
        try:
            for rr_species_key, count in rr_species_counts.items():
                if rr_species_key != "na":
                    (riis_region, gbif_taxon_key) = BisonKey.parse_compound_key(
                        rr_species_key)
                    # Include species name along with internal code
                    try:
                        (accepted_name, species_name) = self._acc_species_name[
                            rr_species_key]
                    except KeyError:
                        raise Exception(f"Missing species name for {rr_species_key}")
                    # Include assessment
                    assessments = riis.get_assessments_for_gbif_taxonkey(gbif_taxon_key)
                    try:
                        assess = assessments[riis_region].lower()
                    except KeyError:
                        assess = "presumed_native"

                    try:
                        # Record contents: LMBISON.region_summary_header()
                        csv_wtr.writerow(
                            [rr_species_key, accepted_name, species_name, count, assess])
                    except Exception as e:
                        self._log.log(
                            f"Unknown write error on {rr_species_key}: {e}",
                            refname=self.__class__.__name__, log_level=logging.ERROR)
        except Exception as e:
            raise Exception(f"Unknown error with {rr_species_key}: {e}")
        finally:
            outf.close()

        return agg_fname

    # ...............................................
    def _get_riis_region(self, state):
        if state in ("AK", "HI"):
            riis_region = state
        else:
            riis_region = "L48"
        return riis_region

    # ...............................................
    def _summarize_region_by_riis(
            self, csvwriter, loc_summary_file, region_type, region_name):
        """Summarize regions by assessment with counts and percentages.

        Args:
            csvwriter (obj): writer object for output summaries
            loc_summary_file (str): filename of summary for region of interest.
            region_type (str): type of region; first value in each tuples contained in
                list of common.constants.REGION[<key>]["summary"]
            region_name (str): name of region; named locations for the type of region

        Returns:
            sp_counts (RIIS_Counts): counts for species
            occ_counts (RIIS_Counts): counts for occurrences

        Raises:
            Exception: on failure to open a csvreader.
            ValueError: on missing "count" value in record.
            Exception: on failure to read a record.
        """
        # Initialize counts by assessment for this region
        assess = {}
        for ass in LMBISON.assess_values():
            assess[ass] = {}

        try:
            rdr, inf = get_csv_dict_reader(
                loc_summary_file, GBIF.DWCA_DELIMITER, encoding=ENCODING,
                quote_none=False)
        except Exception as e:
            raise Exception(f"Unknown open error on {loc_summary_file}: {e}")

        # assess = {"introduced": {rrspkey: count, rrspkey: count, ...},
        #           "invasive": {rrspkey: count, rrspkey: count, ...},
        #           "presumed_native": {rrspkey: count, rrspkey: count, ...}}
        try:
            for rec in rdr:
                try:
                    count = int(rec[LMBISON.COUNT_KEY])
                except ValueError:
                    raise
                assess[rec[LMBISON.ASSESS_KEY]][rec[LMBISON.RR_SPECIES_KEY]] = count
        except Exception as e:
            raise Exception(f"Unknown read error on {loc_summary_file}: {e}")
        finally:
            inf.close()

        sp_counts = RIIS_Counts(self._log, is_group=True)
        occ_counts = RIIS_Counts(self._log, is_group=False)

        # Count introduced/invasive
        for ass in LMBISON.assess_values():
            for _species, count in assess[ass].items():
                if ass == "introduced":
                    sp_counts.introduced += 1
                    occ_counts.introduced += int(count)
                elif ass == "invasive":
                    sp_counts.invasive += 1
                    occ_counts.invasive += int(count)
                elif ass == "presumed_native":
                    sp_counts.presumed_native += 1
                    occ_counts.presumed_native += int(count)

        pct_sp = (
                sp_counts.percent_introduced
                + sp_counts.percent_invasive
                + sp_counts.percent_presumed_native)
        if pct_sp != 1:
            self._log.log(
                f"Percent species totals for {region_type} {region_name}: "
                f"{pct_sp} <> 1.0",
                refname=self.__class__.__name__)

        pct_occ = (
                occ_counts.percent_introduced
                + occ_counts.percent_invasive
                + occ_counts.percent_presumed_native)
        if pct_occ != 1:
            self._log.log(
                f"Percent occ totals for {region_type} {region_name}: {pct_occ} <> 1.0",
                refname=self.__class__.__name__)

        csvwriter.writerow([
            region_type, region_name,
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
    def read_region_assessments(self, full_summary_filename):
        """Read summary data from file, summarize by species and location.

        Args:
            full_summary_filename (str): full filename of occurrence summary of
                annotated records.

        Post condition:
            region_summary_filenames (list): full filenames by region, of summaries of
                counts and percentages of species in that region.
        """
        # Reset location summary and species name lookup
        self._reset_summaries()
        self._read_location_summary(full_summary_filename)
        self._log.log(
            f"Read annotation summary {full_summary_filename} to write by region")

    # ...............................................
    def aggregate_summary_for_regions_assessments(
            self, full_summary_filename, annotated_riis_fname, outpath,
            overwrite=True):
        """Read summary data from file, summarize by species and location.

        Args:
            full_summary_filename (str): full filename of occurrence summary of
                annotated records.
            annotated_riis_fname (str): full filename of RIIS data annotated with
                GBIF accepted taxon.
            outpath (str): full directory path for output filenames.
            overwrite (bool): Flag indicating whether to overwrite existing
                summarized-by-region files.

        Returns:
            region_summary_filenames (list): full filenames by region, of summaries of
                counts and percentages of species in that region.
        """
        # Reset location summary and species name lookup
        self.read_region_assessments(full_summary_filename)

        report = self.aggregate_summary_for_regions(
            annotated_riis_fname, full_summary_filename, outpath,
            overwrite=overwrite)
        report[REPORT.INFILE] = full_summary_filename
        return report

    # ...............................................
    def aggregate_summary_for_regions(
            self, annotated_riis_fname, full_summary_filename, outpath, overwrite=True):
        """Read summary data from file, summarize by species and location.

        Args:
            annotated_riis_fname (str): full filename of RIIS data annotated with
                GBIF accepted taxon.
            full_summary_filename (str): full filename of occurrence summary of
                annotated records.
            outpath (str): full directory path for output filenames.
            overwrite (bool): Flag indicating whether to overwrite existing output files.

        Returns:
            region_summary_filenames (list): full filenames by region, of summaries of
                counts and percentages of species in that region.

        Raises:
            Exception: on aggregated summaries not present.
        """
        summary_msg = self.summary()
        if self.summarized:
            self._log.log(f"Aggregating summary {summary_msg}")
        else:
            raise Exception(
                f"Read species/region summaries before aggregating. Current summary "
                f"is {summary_msg}")
        riis = RIIS(annotated_riis_fname, self._log)
        riis.read_riis()
        report = {}

        # Count occurrences and species by assessment to one line in summary file
        assessment_summary_filename = BisonNameOp.get_assessment_summary_name(
            full_summary_filename, outpath)
        if ready_filename(assessment_summary_filename, overwrite=overwrite):
            try:
                ass_csv_writer, self._outf = get_csv_writer(
                    assessment_summary_filename, GBIF.DWCA_DELIMITER,
                    header=LMBISON.all_summary_header(), fmode="w")
            except Exception as e:
                raise Exception(
                    "Failed to open summary CSV file "
                    f"{assessment_summary_filename}: {e}")
        report["RIIS assessments by location"] = {
            REPORT.OUTFILE: assessment_summary_filename
        }

        for region_type, locations in self._locations.items():
            report[region_type] = {
                REPORT.LOCATION: len(locations),
                REPORT.OUTFILE: []
            }
            for region_value, _species_counts in locations.items():
                # Aggregate region to file
                agg_fname = self._write_region_aggregate(
                    riis, region_type, region_value, outpath, overwrite=overwrite)
                report[region_type][REPORT.OUTFILE].append(agg_fname)
                # Write one line for region to summary of occurrence/species counts
                # and percentages
                # if region_type != LMBISON.SUMMARY_FILTER_HEADING:
                self._summarize_region_by_riis(
                    ass_csv_writer, agg_fname, region_type, region_value)
        return report


# .............................................................................
def summarize_annotations(annotated_filename, output_path, log_path):
    """Summarize data in an annotated GBIF DwC file by state, county, and RIIS.

    Args:
        annotated_filename (str): full filename of an annotated GBIF data file.
        output_path (str): destination directory for output files.
        log_path (str): destination directory for logfile

    Returns:
        summary_filename (str): full filename of a summary file

    Raises:
        FileNotFoundError: on missing input file
    """
    if not os.path.exists(annotated_filename):
        raise FileNotFoundError(annotated_filename)

    datapath, basefname = os.path.split(annotated_filename)
    refname = f"summarize_{basefname}"
    logger = Logger(refname, log_filename=os.path.join(log_path, f"{refname}.log"))
    logger.log(f"Submit {basefname} for summarizing.", refname=refname)

    logger.log(f"Start Time : {datetime.now()}", refname=refname)
    agg = Aggregator(logger)
    summary_filename = BisonNameOp.get_process_outfilename(
        annotated_filename, outpath=output_path, step_or_process=LMBISON_PROCESS.AGGREGATE)

    # Do not overwrite existing summary
    agg.summarize_annotated_recs_by_location(annotated_filename, summary_filename, overwrite=False)

    logger.log(f"End Time : {datetime.now()}", refname=refname)
    return summary_filename


# # .............................................................................
# def summarize_summaries(annotated_filename, output_path, log_path):
#     """Summarize data in one annotated GBIF DwC file by state, county, and RIIS.
#
#     Args:
#         annotated_filename (str): full filename of an annotated GBIF data file.
#         output_path (str): destination directory for output files.
#         log_path (str): destination directory for logfile
#
#     Returns:
#         summary_filename (str): full filename of a summary file
#
#     Raises:
#         FileNotFoundError: on missing input file
#     """
#     if not os.path.exists(annotated_filename):
#         raise FileNotFoundError(annotated_filename)
#
#     datapath, basefname = os.path.split(annotated_filename)
#     refname = f"summarize_{basefname}"
#     logger = Logger(refname, log_filename=os.path.join(log_path, f"{refname}.log"))
#     logger.log(f"Submit {basefname} for summarizing.", refname=refname)
#
#     logger.log(f"Start Time : {datetime.now()}", refname=refname)
#     agg = Aggregator(logger)
#     summary_filename = BisonNameOp.get_process_outfilename(
#         annotated_filename, outpath=output_path, step_or_process=LMBISON_PROCESS.AGGREGATE)
#
#     # Do not overwrite existing summary
#     agg.summarize_annotated_recs_by_location(
#         annotated_filename, summary_filename, overwrite=False)
#
#     logger.log(f"End Time : {datetime.now()}", refname=refname)
#     return summary_filename


# .............................................................................
def parallel_summarize(annotated_filenames, output_path, main_logger):
    """Main method for parallel execution of summarization script.

    Args:
        annotated_filenames (list): list of full filenames containing annotated
            GBIF data.
        output_path (str): destination directory for output files.
        main_logger (logger): logger for the process that calls this function,
            initiating subprocesses

    Returns:
        annotated_dwc_fnames (list): list of full output filenames
    """
    refname = "parallel_summarize"
    inputs = []
    for in_csv in annotated_filenames:
        out_csv = BisonNameOp.get_process_outfilename(
            in_csv, outpath=output_path, step_or_process=LMBISON_PROCESS.SUMMARIZE)
        if os.path.exists(out_csv):
            main_logger.info(
                f"Summaries exist in {out_csv}, moving on.", refname=refname)
        else:
            inputs.append((in_csv, main_logger.log_directory))

    main_logger.info(
        f"Parallel Summarize Start Time : {datetime.now()}", refname=refname)
    # Do not use all CPUs
    pool = multiprocessing.Pool(available_cpu_count() - 2)
    # Map input files asynchronously onto function
    map_result = pool.starmap_async(summarize_annotations, inputs)
    # Wait for results
    map_result.wait()
    summary_filenames = map_result.get()
    main_logger.log(f"Parallel Summarize End Time : {datetime.now()}", refname=refname)

    return summary_filenames


# .............................................................................
__all__ = [
    "Aggregator",
    "parallel_summarize",
    "RIIS_Counts",
    "summarize_annotations"
]
