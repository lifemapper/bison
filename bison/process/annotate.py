"""Common classes for adding USGS RIIS info to GBIF occurrences."""
import logging
import time
from multiprocessing import Pool
import os
from logging import ERROR
import time

from bison.common.constants import (
    APPEND_TO_DWC, LMBISON_PROCESS, ENCODING, GBIF, LOG, REGION, REPORT)
from bison.common.log import Logger
from bison.common.util import (available_cpu_count, BisonNameOp, get_csv_dict_writer)
from bison.process.geoindex import GeoResolver
from bison.provider.gbif_data import DwcData
from bison.provider.riis_data import RIIS


# .............................................................................
class Annotator():
    """Class for adding USGS RIIS info to GBIF occurrences."""
    def __init__(
            self, geo_input_path, logger, riis_with_gbif_filename=None, riis=None):
        """Constructor.

        Args:
            geo_input_path (str): input path for geospatial files to intersect points
            logger (object): logger for saving relevant processing messages
            riis_with_gbif_filename (str): full filename of RIIS data annotated with
                GBIF accepted taxon names.
            riis (bison.common.riis.RIIS): object containing USGS RIIS data for
                annotating records

        Raises:
            Exception: on RIIS without annotations provided, and no
                annotated_riis_filename for writing an annotated RIIS file.
            Exception: on neither annotated_riis_filename nor RIIS provided

        Notes:
            constructor creates spatial indices for the geospatial files for
                geographic areas of interest.
            geographic areas are in bison.common.constants variables and include:
                * state and county from census boundaries in US_CENSUS_COUNTY
                * region names from American Indian/Alaska Native Areas/Hawaiian Home
                    Lands in US_AIANNH
                * protected areas from the Protected areas Database in US_PAD
            US-RIIS determinations are calculated from the species/state combination.
        """
        self._log = logger
        self._csvfile = None
        self._datapath = None
        self._dwcdata = None
        self._csv_writer = None
        self._outf = None

        if riis is not None:
            self.riis = riis
            if self.riis.is_annotated is False:
                if riis_with_gbif_filename is None:
                    raise Exception(
                        "RIIS data does not contain GBIF accepted taxa, needed for "
                        "annotating GBIF occurrence data.")
                else:
                    self.riis.resolve_riis_to_gbif_taxa(
                        riis_with_gbif_filename, overwrite=True)
        elif riis_with_gbif_filename is not None:
            self.riis = RIIS(riis_with_gbif_filename, logger)
            self.riis.read_riis()
        else:
            raise Exception(
                "Must provide either RIIS with annotations or riis_with_gbif_filename")

        # Geospatial indexes for regional attributes to add to records
        self._geo_fulls = []
        for region in REGION.full_region():
            fn = os.path.join(geo_input_path, region["file"])
            self._geo_fulls.append(GeoResolver(
                fn, region["map"], self._log, is_disjoint=region["is_disjoint"],
                buffer_vals=region["buffer"]))

        # Geospatial indexes for subset regions of a whole, all with same
        # attributes to add
        self._geo_partials = {}
        region = REGION.combine_to_region()
        for filter_val, rel_fn in region["files"]:
            fn = os.path.join(geo_input_path, rel_fn)
            self._geo_partials[filter_val] = GeoResolver(
                fn, region["map"], self._log, is_disjoint=region["is_disjoint"],
                buffer_vals=region["buffer"])

    # ...............................................
    def initialize_occurrences_io(self, gbif_occ_filename, output_occ_filename):
        """Initialize and open required input and output files of occurrence records.

        Also reads the first record and writes the header.

        Args:
            gbif_occ_filename (str): full filename input DwC occurrence file.
            output_occ_filename (str): destination full filename for the output
                annotated occurrence records.

        Raises:
            Exception: on failure to open the DwcData csvreader.
            Exception: on failure to open the csv_writer.

        Note:
            Initializes or resets member attributes for new data:
              file-related: _csvfile, _datapath, _csv_writer, _outf
              dwc data reader: _dwcdata
              reporting data: bad_ranks, rank_filtered_records
        """
        self.close()
        if gbif_occ_filename is not None:
            if not os.path.exists(gbif_occ_filename):
                raise Exception(f"Missing input occurrence data {gbif_occ_filename}")
            if output_occ_filename is None:
                raise Exception(
                    "Must provide an output filename to write annotated input "
                    f"data in {gbif_occ_filename}")

            # Reset reporting attributes
            self.bad_ranks = set()
            self.rank_filtered_records = 0

            # Reset file attributes for new data
            self._datapath, _ = os.path.split(gbif_occ_filename)
            self._csvfile = gbif_occ_filename
            self._output_csvfile = output_occ_filename
            # Input reader
            self._dwcdata = DwcData(self._csvfile, logger=self._log)
            try:
                self._dwcdata.open()
            except Exception:
                raise

            header = self._dwcdata.fieldnames
            for fld in APPEND_TO_DWC.annotation_fields():
                header.append(fld)
            try:
                self._csv_writer, self._outf = get_csv_dict_writer(
                    self._output_csvfile, header, GBIF.DWCA_DELIMITER, fmode="w",
                    encoding=ENCODING, overwrite=True)
            except Exception:
                raise Exception(
                    f"Failed to open file or csv_writer for {self._output_csvfile}")

    # ...............................................
    def close(self):
        """Close input datafiles and output file."""
        self._csv_writer = None
        try:
            self._dwcdata.close()
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
        if ((self._dwcdata is not None and self._dwcdata.is_open)
                or (self._outf is not None and not self._outf.closed)):
            return True
        return False

    # ...............................................
    def _filter_find_taxon_keys(self, dwcrec):
        """Returns acceptedTaxonKey for species and a lower rank key where determined.

        Args:
            dwcrec: one original GBIF DwC record

        Returns:
            taxkeys (list of int): if the record is Species rank or below, return the
                accepted species GBIF taxonKey and, if lower than species, the lower
                rank acceptedTaxonKey.
        """
        taxkeys = []
        # Identify whether this record is above Species rank
        # (exclude higher level determinations from resolution and summary)
        if dwcrec[GBIF.RANK_FLD].lower() not in GBIF.ACCEPT_RANK_VALUES:
            self.bad_ranks.add(dwcrec[GBIF.RANK_FLD])
            self.rank_filtered_records += 1
        # Species or below
        else:
            # Find RIIS records for this acceptedTaxonKey.
            taxkeys.append(dwcrec[GBIF.ACC_TAXON_FLD])
            if (dwcrec[GBIF.RANK_FLD].lower() != "species"
                    and dwcrec[GBIF.ACC_TAXON_FLD] != dwcrec[GBIF.SPECIES_KEY_FLD]):
                # If acceptedTaxonKey is below species, find RIIS records for species
                # key too.
                taxkeys.append(dwcrec[GBIF.SPECIES_KEY_FLD])

        return taxkeys

    # ...............................................
    def _annotate_one_record(self, dwcrec):
        """Append fields to GBIF record, then write to file.

        Args:
            dwcrec: one original GBIF DwC record

        Returns:
            dwcrec: one GBIF DwC record, if it is not filtered out, values are added
                to calculated fields: APPEND_TO_DWC, otherwise these fields are
                None.
        """
        gbif_id = dwcrec[GBIF.ID_FLD]

        # Leave these fields None if the record is filtered out (rank above species)
        filtered_taxkeys = self._filter_find_taxon_keys(dwcrec)

        # Only append additional values to records that pass the filter tests.
        if not filtered_taxkeys:
            dwcrec[APPEND_TO_DWC.FILTER_FLAG] = True
        else:
            dwcrec[APPEND_TO_DWC.FILTER_FLAG] = False
            lon = dwcrec[GBIF.LON_FLD]
            lat = dwcrec[GBIF.LAT_FLD]

            for georesolver in self._geo_fulls:
                # Find enclosing region and its attributes
                try:
                    fldvals = georesolver.find_enclosing_polygon_attributes(lon, lat)
                except ValueError as e:
                    self._log.log(
                        f"Record gbifID: {gbif_id}: {e}",
                        refname=self.__class__.__name__, log_level=logging.ERROR)
                else:
                    # Add values to record
                    for fld, val in fldvals.items():
                        dwcrec[fld] = val

            # Find RIIS region from resolved state
            state = dwcrec[APPEND_TO_DWC.RESOLVED_ST]
            riis_region = "L48"
            if state in ("AK", "HI"):
                riis_region = state

            # Find any RIIS records for these acceptedTaxonKeys and region.
            (riis_assessment,
             riis_key) = self.riis.get_assessment_for_gbif_taxonkeys_region(
                filtered_taxkeys, riis_region)
            dwcrec[APPEND_TO_DWC.RIIS_ASSESSMENT] = riis_assessment
            dwcrec[APPEND_TO_DWC.RIIS_KEY] = riis_key

            # Find subset area from partial data indicated by the filter index
            filter_field = REGION.combine_to_region()["filter_field"]
            filter = dwcrec[filter_field]
            try:
                sub_resolver = self._geo_partials[filter]
            except KeyError:
                self._log.log(
                    f"No datafile exists for partial region {filter}", refname=self.__class__.__name__,
                    log_level=logging.ERROR)
            else:
                # Find enclosing PAD if exists, and its attributes
                try:
                    fldvals = sub_resolver.find_enclosing_polygon_attributes(lon, lat)
                except ValueError as e:
                    self._log.log(
                        f"Record gbifID: {gbif_id}: {e}",
                        refname=self.__class__.__name__, log_level=logging.ERROR)
                else:
                    # Add values to record
                    for fld, val in fldvals.items():
                        dwcrec[fld] = val

        return dwcrec

    # ...............................................
    def annotate_dwca_records(self, dwc_filename, output_path):
        """Resolve and append state, county, RIIS assessment and key to GBIF records.

        Args:
            dwc_filename: full filename of input file of DWC records.
            output_path (str): destination directory for annotated occurrence file.

        Returns:
            report: dictionary of metadata about the data and process

        Raises:
            Exception: on failure to open input or output data.
            Exception: on unexpected failure to read or write data.
        """
        start = time.perf_counter()
        self._log.log(
            f"Start Time : {time.asctime()}", refname=self.__class__.__name__)

        out_filename = BisonNameOp.get_process_outfilename(
            dwc_filename, outpath=output_path, step_or_process=LMBISON_PROCESS.ANNOTATE)
        self.initialize_occurrences_io(dwc_filename, out_filename)
        self._log.log(
            f"Annotate {dwc_filename} to {out_filename}", refname=self.__class__.__name__)

        report = {
            REPORT.INFILE: dwc_filename,
            REPORT.OUTFILE: out_filename,
            REPORT.ANNOTATE_FAIL: []
        }
        try:
            # iterate over DwC records
            dwcrec = self._dwcdata.get_record()
            while dwcrec is not None:
                # Annotate
                dwcrec_ann = self._annotate_one_record(dwcrec)
                # Write
                try:
                    self._csv_writer.writerow(dwcrec_ann)
                except Exception as e:
                    self._log.log(
                        f"Error {e} record, gbifID {dwcrec[GBIF.ID_FLD]}",
                        refname=self.__class__.__name__, log_level=ERROR)
                    report[REPORT.ANNOTATE_FAIL].append(dwcrec[GBIF.ID_FLD])

                if (self._dwcdata.recno % LOG.INTERVAL) == 0:
                    self._log.log(
                        f"*** Record number {self._dwcdata.recno}, gbifID: "
                        f"{dwcrec[GBIF.ID_FLD]} ***", refname=self.__class__.__name__)

                # Get next
                dwcrec = self._dwcdata.get_record()
        except Exception as e:
            raise Exception(
                f"Unexpected error {e} reading {self._dwcdata.input_file} or "
                + f"writing {out_filename}")

        report[REPORT.RANK_FAIL] = list(self.bad_ranks)
        report[REPORT.RANK_FAIL_COUNT] = self.rank_filtered_records
        end = time.perf_counter()
        self._log.log(
            f"Annotate records filtered out {self.rank_filtered_records} " +
            f"records with {self.bad_ranks} ranks", refname=self.__class__.__name__)
        self._log.log(
            f"End Time : {time.asctime()}, duration {end - start} seconds",
            refname=self.__class__.__name__)

        return report


# .............................................................................
def annotate_occurrence_file(
        dwc_filename, riis_annotated_filename, geo_path, output_path, log_path):
    """Annotate GBIF records with census state and county, and RIIS key and assessment.

    Args:
        dwc_filename (str): full filename containing GBIF data for annotation.
        riis_annotated_filename (str): full filename with RIIS records annotated with
            gbif accepted taxa
        geo_path (str): input directory containing geospatial files for
            geo-referencing occurrence points.
        output_path (str): destination directory for output annotated occurrence files.
        log_path (object): destination directory for logging processing messages.

    Returns:
        report (dict): metadata for the occurrence annotation data and process.

    Raises:
        FileNotFoundError: on missing input file
    """
    if not os.path.exists(dwc_filename):
        raise FileNotFoundError(dwc_filename)

    logname, log_fname = BisonNameOp.get_process_logfilename(
        dwc_filename, logpath=log_path, step_or_process=LMBISON_PROCESS.ANNOTATE)
    logger = Logger(
        logname, log_filename=log_fname, log_console=False)

    logger.log(
        f"Submit {dwc_filename} for annotation {time.asctime()}",
        refname="annotate_occurrence_file")

    ant = Annotator(
        geo_path, logger, riis_with_gbif_filename=riis_annotated_filename)
    report = ant.annotate_dwca_records(dwc_filename, output_path)

    logger.log(
        f"End Time : {time.asctime()}", refname="annotate_occurrence_file")
    return report


# .............................................................................
def parallel_annotate(
        dwc_filenames, riis_with_gbif_filename, geo_path, output_path, main_logger):
    """Main method for parallel execution of DwC annotation script.

    Args:
        dwc_filenames (list): list of full filenames containing GBIF data for
            annotation.
        riis_with_gbif_filename (str): full filename with RIIS records annotated with
            gbif accepted taxa
        geo_path (str): input directory containing geospatial files for
            geo-referencing occurrence points.
        output_path (str): destination directory for output annotated occurrence files.
        main_logger (logger): logger for the process that calls this function,
            initiating subprocesses

    Returns:
        reports (list of dict): metadata for the occurrence annotation data and process.
    """
    refname = "parallel_annotate"
    messages = []
    inputs = []
    if output_path is None:
        output_path = os.path.dirname(dwc_filenames[0])
    log_path = main_logger.log_directory

    # Process only needed files
    for dwc_fname in dwc_filenames:
        out_fname = BisonNameOp.get_process_outfilename(
            dwc_fname, outpath=output_path, step_or_process=LMBISON_PROCESS.ANNOTATE)
        if os.path.exists(out_fname):
            msg = f"Annotations exist in {out_fname}."
            main_logger.log(msg, refname=refname)
            messages.append(msg)
        else:
            inputs.append(
                (dwc_fname, riis_with_gbif_filename, geo_path, output_path, log_path))
    main_logger.log(
        f"Parallel Annotation Start Time : {time.asctime()}", refname=refname)

    # Do not use all CPUs
    pool = Pool(available_cpu_count() - 2)
    # Map input files asynchronously onto function
    map_result = pool.starmap_async(annotate_occurrence_file, inputs)
    # Wait for results
    map_result.wait()
    # list of reports for each process
    reports = map_result.get()

    report = {
        "reports": reports,
        "messages": messages
    }

    main_logger.log(
        f"Parallel Annotation End Time : {time.asctime()}", refname=refname)

    return report


# .............................................................................
__all__ = [
    "annotate_occurrence_file",
    "parallel_annotate"
]
