"""Common classes for adding USGS RIIS info to GBIF occurrences."""
import os

from bison.common.constants import (
    AGGREGATOR_DELIMITER, ENCODING, GBIF, NEW_RESOLVED_COUNTY, NEW_RESOLVED_STATE, NEW_RIIS_ASSESSMENT_FLD)
from bison.tools.util import (get_csv_writer, get_csv_dict_reader, get_logger)

SPECIES_KEY = "species_key"
ASSESS_KEY = "assessment"
LOCATION_KEY = "location_key"
COUNT_KEY = "count"
SUMMARY_HEADER = [SPECIES_KEY, ASSESS_KEY, LOCATION_KEY, COUNT_KEY]


# .............................................................................
class Aggregator():
    """Class for summarizing GBIF data annotated with USGS Introduced and Invasive species assessments."""
    def __init__(self, datapath, base_filename_list, logger=None):
        """Constructor.

        Args:
            datapath (str): base directory for datafiles
            base_filename_list (list): base filenames for annotated GBIF occurrence CSV file
            logger (object): logger for saving relevant processing messages
        """
        self._datapath = datapath
        self._csvfiles = [os.path.join(datapath, fname) for fname in base_filename_list]

        if logger is None:
            logger = get_logger(datapath)
        self._log = logger
        #  {gbif_sciname: {introduced: {state-county: count, ...},
        #                  invasive: {state-county: count, ...},
        #                  presumed-native: {state-county: count, ...},
        #                  gbif_taxon_key: taxonKey,
        #                  itis_scientific_name: tsn,
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
    def _add_record_to_summary(self, species_key, assess, location_key):
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
            self.species[species_key] = {assess: {location_key: 1}}

    # ...............................................
    def _summarize_annotations(self, annotated_occurrence_filename):
        csv_rdr, inf = get_csv_dict_reader(
            annotated_occurrence_filename, GBIF.DWCA_DELIMITER, fieldnames=None, encoding=ENCODING, ignore_quotes=True)
        try:
            for rec in csv_rdr:
                species_key = f"{rec[GBIF.ACC_TAXON_FLD]}{AGGREGATOR_DELIMITER}{rec[GBIF.ACC_NAME_FLD]}"
                location_key = f"{rec[NEW_RESOLVED_STATE]}{AGGREGATOR_DELIMITER}{rec[NEW_RESOLVED_COUNTY]}"

                assess = rec[NEW_RIIS_ASSESSMENT_FLD].lower()
                if assess not in ("introduced", "invasive"):
                    assess = "presumed_native"

                self._add_record_to_summary(species_key, assess, location_key)

        except Exception as e:
            raise Exception(f"Failed to read annotated occurrences from {annotated_occurrence_filename}: {e}")

        finally:
            csv_rdr = None
            inf.close()

    # ...............................................
    def _write_summary(self, summary_filename):
        csv_wtr, outf = get_csv_writer(summary_filename, GBIF.DWCA_DELIMITER, fmode="w")
        csv_wtr.writerow(SUMMARY_HEADER)

        for species_key, sp_info in self.species.items():
            for assessment, location_info in sp_info.items():
                for location_key, count in location_info.items():
                    row = [species_key, assessment, location_key, count]
                    csv_wtr.writerow(row)

    # ...............................................
    def _read_write_summary(self, annotated_occurrence_filename, summary_filename):
        self._summarize_annotations(annotated_occurrence_filename)
        self._write_summary(summary_filename)

    # ...............................................
    def aggregate_summarize(self, location_summary_filename, species_summary_filename):
        """Read annotated data from one or more files, summarize by species, and by location.

        Args:
            location_summary_filename (str): full output filename for the summary by county and state
            species_summary_filename (str): full output filename for the summary by species

        Raises:
            Exception: on unexpected read error
        """
        # Clear existing summary
        self.species = {}

        # Read annotated occurrences and summarize each file
        summary_files = []
        for annotated_occurrence_filename in self._csvfiles:
            basename, ext = os.path.splitext(annotated_occurrence_filename)
            summary_filename = f"{basename}_summary{ext}"
            self._read_write_summary(annotated_occurrence_filename, summary_filename)
            summary_files.append(summary_filename)

        # Read summaries of occurrences and aggregate
        for sum_fname in summary_files:
            csv_rdr, inf = get_csv_dict_reader(
                sum_fname, GBIF.DWCA_DELIMITER, fieldnames=SUMMARY_HEADER, encoding=ENCODING, ignore_quotes=True)
            try:
                for rec in csv_rdr:
                    self._add_record_to_summary(rec[SPECIES_KEY], rec[ASSESS_KEY], rec[LOCATION_KEY])

            except Exception as e:
                raise Exception(f"Failed to read summarized occurrences from {sum_fname}: {e}")

            finally:
                csv_rdr = None
                inf.close()

        # Write summary of all files
        self._write_location_summary(location_summary_filename)
        self._write_species_summary(species_summary_filename)
