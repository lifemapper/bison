"""Common classes for adding USGS RIIS info to GBIF occurrences."""
import os

from bison.common.constants import (
    AGGREGATOR_DELIMITER, ENCODING, GBIF, NEW_RESOLVED_COUNTY, NEW_RESOLVED_STATE, NEW_RIIS_ASSESSMENT_FLD)
from bison.tools.util import (get_csv_writer, get_csv_dict_reader, get_logger)

GBIF_TAXON_KEY = "gbif_taxon_key"
SPECIES_KEY = "species_key"
ASSESS_KEY = "assessment"
LOCATION_KEY = "location_key"
COUNT_KEY = "count"
SUMMARY_HEADER = [SPECIES_KEY, GBIF_TAXON_KEY, ASSESS_KEY, LOCATION_KEY, COUNT_KEY]


# .............................................................................
class Aggregator():
    """Class for summarizing GBIF data annotated with USGS Introduced and Invasive species assessments."""
    def __init__(self, annotated_filename, logger=None):
        """Constructor.

        Args:
            occ_filename_list (list): full filenames for annotated occurrence CSV files
            logger (object): logger for saving relevant processing messages

        Raises:
            Exception: on no files to process
            FileNotFoundError: on any missing file in occ_filename_list
        """
        if not os.path.exists(annotated_filename):
            raise FileNotFoundError(f"File {annotated_filename} does not exist")

        datapath, _ = os.path.split(annotated_filename)
        self._datapath = datapath
        self._csvfile = annotated_filename
        if logger is None:
            logger = get_logger(datapath)
        self._log = logger
        #  {gbif_sciname: {introduced: {state-county: count, ...},
        #                  invasive: {state-county: count, ...},
        #                  presumed-native: {state-county: count, ...},
        #                  gbif_taxon_key: taxonKey
        #                  }
        #   }
        self.species = {}

    # ...............................................
    def _write_location_summary(self, location_summary_filename):
        pass

    # ...............................................
    def _write_species_summary(self, species_summary_filename):
        pass

    # ...............................................
    def _add_record_to_summary(self, species_key, assess, location_key, gbif_taxon_key=None):
        # Is species present?
        try:
            self.species[species_key]

            # Is assessment present?
            try:
                self.species[species_key][assess]

                # Is location present?
                try:
                    # Increment location count for this assessment
                    self.species[species_key][assess][location_key] += 1

                except KeyError:
                    # Add a location count for this assessment
                    self.species[species_key][assess][location_key] = 1

            # Add new assessment and location
            except KeyError:
                self.species[species_key][assess] = {location_key: 1}

        # Add new species, assessment and location
        except KeyError:
            self.species[species_key] = {assess: {location_key: 1},
                                         GBIF_TAXON_KEY: gbif_taxon_key}

    # ...............................................
    def summarize_annotations(self):
        # Reset summary
        self.species = {}
        csv_rdr, inf = get_csv_dict_reader(
            self._csvfile, GBIF.DWCA_DELIMITER, encoding=ENCODING, ignore_quotes=True)
        try:
            for rec in csv_rdr:
                species_key = f"{rec[GBIF.ACC_TAXON_FLD]}{AGGREGATOR_DELIMITER}{rec[GBIF.ACC_NAME_FLD]}"
                location_key = f"{rec[NEW_RESOLVED_STATE]}{AGGREGATOR_DELIMITER}{rec[NEW_RESOLVED_COUNTY]}"

                assess = rec[NEW_RIIS_ASSESSMENT_FLD].lower()
                if assess not in ("introduced", "invasive"):
                    assess = "presumed_native"

                self._add_record_to_summary(species_key, assess, location_key, gbif_taxon_key=rec[GBIF.ACC_TAXON_FLD])

        except Exception as e:
            raise Exception(f"Failed to read annotated occurrences from {self._csvfile}: {e}")

        finally:
            csv_rdr = None
            inf.close()

    # ...............................................
    def _write_summary(self, summary_filename):
        try:
            csv_wtr, outf = get_csv_writer(summary_filename, GBIF.DWCA_DELIMITER, fmode="w")
            csv_wtr.writerow(SUMMARY_HEADER)

            for species_key, sp_info in self.species.items():
                gbif_taxon_key = sp_info[GBIF_TAXON_KEY]
                for assessment, location_info in sp_info.items():
                    # Taxon key included with assessments
                    if assessment != GBIF_TAXON_KEY:
                        for location_key, count in location_info.items():
                            row = [species_key, gbif_taxon_key, assessment, location_key, count]
                            csv_wtr.writerow(row)
        except Exception as e:
            raise Exception(f"Failed to write summary file {summary_filename}: {e}")
        finally:
            outf.close()

    # ...............................................
    def summarize_to_file(self):
        """Read an annotated file, summarize by species and location, write to a csvfile.

        Returns:
            summary_filename: full output filename for CSV summary of annotated occurrence file. CSV fields are
                 [SPECIES_KEY, GBIF_TAXON_KEY, ASSESS_KEY, LOCATION_KEY, COUNT_KEY].
        """
        basename, ext = os.path.splitext(self._csvfile)
        summary_filename = f"{basename}_summary{ext}"
        # Summarize and write
        self.summarize_annotations(self._csvfile)
        self._write_summary(summary_filename)
        return summary_filename

    # ...............................................
    def read_summaries(self, summary_filename_list):
        """Read annotated data from a summary file and populate the self.species dictionary.

        Args:
            summary_filename_list (list): list of full summary input filenames to be read.

        Raises:
            Exception: on unexpected read error
        """
        self.species = {}

        # Read summaries of occurrences and aggregate
        for summary_filename in summary_filename_list:
            try:
                csv_rdr, inf = get_csv_dict_reader(
                    summary_filename, GBIF.DWCA_DELIMITER, fieldnames=SUMMARY_HEADER, encoding=ENCODING, ignore_quotes=True)
                for rec in csv_rdr:
                    self._add_record_to_summary(
                        rec[SPECIES_KEY], rec[ASSESS_KEY], rec[LOCATION_KEY], gbif_taxon_key=rec[GBIF_TAXON_KEY])
            except Exception as e:
                raise Exception(f"Failed to read summary file {summary_filename}: {e}")
            finally:
                inf.close()

    # ...............................................
    def structure_summary(self):
        """Read annotated data from one or more files, summarize by species, and by location.

        Args:
            location_summary_filename (str): full output filename for the summary by county and state
            species_summary_filename (str): full output filename for the summary by species

        Raises:
            Exception: on unexpected read error
        """
        if not self.species:
            raise Exception("No summarized data to structure, first summarize_annotations or summarize_to_file")


    # ...............................................
    def summarize_stats(self, location_summary_filename, species_summary_filename):
        """Read annotated data from one or more files, summarize by species, and by location.

        Args:
            location_summary_filename (str): full output filename for the summary by county and state
            species_summary_filename (str): full output filename for the summary by species

        Raises:
            Exception: on unexpected read error
        """

        # Write summary of all files
        self._write_location_summary(location_summary_filename)
        self._write_species_summary(species_summary_filename)
