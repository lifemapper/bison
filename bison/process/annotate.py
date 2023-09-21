"""Common classes for adding USGS RIIS info to GBIF occurrences."""
from concurrent.futures import as_completed, ProcessPoolExecutor
import logging
import os
from logging import ERROR
import time

from bison.common.constants import (
    APPEND_TO_DWC, ENCODING, GBIF, LMBISON_PROCESS, LOG, REGION, REPORT)
from bison.common.log import Logger
from bison.common.util import (available_cpu_count, BisonNameOp, get_csv_dict_writer)
from bison.process.geoindex import get_geo_resolvers
from bison.provider.gbif_data import DwcData
from bison.provider.riis_data import RIIS


# .............................................................................
class Annotator():
    """Class for adding USGS RIIS info to GBIF occurrences."""
    def __init__(
            self, logger, geo_path, annotated_riis_filename=None, riis=None,
            regions=None):
        """Constructor.

        Args:
            logger (object): logger for saving relevant processing messages
            geo_path (str): input path for geospatial files to intersect points
            annotated_riis_filename (str): full filename of RIIS data annotated with
                GBIF accepted taxon names.
            riis (bison.common.riis.RIIS): object containing USGS RIIS data for
                annotating records
            regions (list of common.constants.REGION): sequence of REGION members for
                intersection.  If None, use all.

        Raises:
            Exception: on RIIS without annotations provided, and no
                annotated_riis_filename for writing an annotated RIIS file.
            Exception: on neither annotated_riis_filename nor RIIS provided
            Exception: on not returning the requested georesolvers.

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
        # Geospatial indexes for regional attributes to add to records
        self._regions = regions
        if regions is None:
            self._regions = REGION.for_resolve()
        self._geo_fulls, self._pad_states = get_geo_resolvers(
            geo_path, self._regions, self._log)

        # Check # resolvers == # regions
        subset_count = 0
        if self._pad_states:
            subset_count = 1
        if len(self._geo_fulls) + subset_count != len(self._regions):
            raise Exception(
                f"Found {len(self._geo_fulls) + len(self._pad_states)} georesolvers"
                f"does not equal requested georesolvers ({len(self._regions)})")

        if riis is not None:
            self.riis = riis
            if self.riis.is_annotated is False:
                if annotated_riis_filename is None:
                    raise Exception(
                        "RIIS data does not contain GBIF accepted taxa, needed for "
                        "annotating GBIF occurrence data.")
                else:
                    self.riis.resolve_riis_to_gbif_taxa(
                        annotated_riis_filename, overwrite=True)
        elif annotated_riis_filename is not None:
            self.riis = RIIS(annotated_riis_filename, logger)
            self.riis.read_riis()
        else:
            raise Exception(
                "Must provide either RIIS with annotations or annotated_riis_filename")

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
                if fld not in header:
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
    def _log_rec_vals(self, dwcrec):
        s = ""
        for fn in [
            GBIF.ID_FLD, GBIF.LON_FLD, GBIF.LAT_FLD, "stateProvince",
            APPEND_TO_DWC.RESOLVED_ST,
            APPEND_TO_DWC.RIIS_ASSESSMENT, APPEND_TO_DWC.AIANNH_NAME,
            APPEND_TO_DWC.PAD_NAME, APPEND_TO_DWC.PAD_GAP_STATUS
        ]:
            s = (f"{s}\n{fn}: {dwcrec[fn]}")
            self._log.log(s, refname=self.__class__.__name__, log_level=logging.WARNING)

    # ...............................................
    def _annotate_one_record(self, dwcrec, taxkeys, do_riis=True):
        """Append fields to GBIF record, then write to file.

        Args:
            dwcrec (dict): one original GBIF DwC record
            taxkeys (list of str): list of taxon keys for this record.
            do_riis (bool): flag indicating whether to annotate with RIIS assessment.

        Modifies:
            dwcrec: adds fields to GBIF DwC record.
        """
        gbif_id = dwcrec[GBIF.ID_FLD]
        lon = dwcrec[GBIF.LON_FLD]
        lat = dwcrec[GBIF.LAT_FLD]
        # if gbif_id in [
        #         "1861183762", "2978848311", "1212531557", "1212531572", "1212531578"
        # ]:
        #     self._log.log(
        #         f"Test gbifID: {gbif_id}", refname=self.__class__.__name__)

        for georesolver in self._geo_fulls:
            # Find enclosing region and its attributes
            try:
                fldval_list = georesolver.find_enclosing_polygon_attributes(lon, lat)
            except ValueError as e:
                self._log.log(
                    f"Record gbifID: {gbif_id}: {e}",
                    refname=self.__class__.__name__, log_level=logging.ERROR)
            else:
                # These should only return values for one feature, take the first
                if fldval_list:
                    fldvals = fldval_list[0]
                    # Add values to record
                    for fld, val in fldvals.items():
                        dwcrec[fld] = val

        if do_riis is True:
            # Find RIIS region from resolved state
            state = dwcrec[APPEND_TO_DWC.RESOLVED_ST]
            # TODO: When is state resolution failing??
            if state is None or state == "":
                self._log.log(
                    f"No state for gbifID: {gbif_id}:", refname=self.__class__.__name__,
                    log_level=logging.ERROR)
            riis_region = "L48"
            if state in ("AK", "HI"):
                riis_region = state

            # Find any RIIS records for these acceptedTaxonKeys and region.
            (riis_assessment,
             riis_key) = self.riis.get_assessment_for_gbif_taxonkeys_region(
                taxkeys, riis_region)
            dwcrec[APPEND_TO_DWC.RIIS_ASSESSMENT] = riis_assessment
            dwcrec[APPEND_TO_DWC.RIIS_KEY] = riis_key

        # TODO: THIS IS PAD-SPECIFIC, b/c there may be more than one intersecting PAD
        # polygon.  Choose the first one with the most protective GAP Status
        # Assume only ONE dataset, PAD, with subset components (geo_subsets)
        if self._pad_states:
            # Find subset area from partial data indicated by the filter index
            subset_field = REGION.PAD["subset_field"]
            filter = dwcrec[subset_field]
            try:
                sub_resolver = self._pad_states[filter]
            except KeyError:
                self._log.log(
                    f"No datafile exists for partial region {filter}",
                    refname=self.__class__.__name__,
                    log_level=logging.ERROR)
            else:
                # Find enclosing PAD if exists, and its attributes
                try:
                    fldval_list = sub_resolver.find_enclosing_polygon_attributes(
                        lon, lat)
                except ValueError as e:
                    self._log.log(
                        f"Record gbifID: {gbif_id}: {e}",
                        refname=self.__class__.__name__, log_level=logging.ERROR)
                else:
                    if fldval_list:
                        # May return many - get first feature with GAP status for most
                        # protection (lowest val)
                        retvals = fldval_list[0]
                        if len(fldval_list) > 1:
                            for fv in fldval_list:
                                if (
                                        fv[APPEND_TO_DWC.PAD_GAP_STATUS] <
                                        retvals[APPEND_TO_DWC.PAD_GAP_STATUS]
                                ):
                                    retvals = fv
                        # Add values to record
                        for fld, val in retvals.items():
                            dwcrec[fld] = val
        if state == "":
            self._log_rec_vals(dwcrec)

    # ...............................................
    def annotate_dwca_records(self, dwc_filename, output_path, overwrite=False):
        """Resolve and append state, county, RIIS assessment and key to GBIF records.

        Args:
            dwc_filename: full filename of input file of DWC records.
            output_path (str): destination directory for annotated occurrence file.
            overwrite (bool): Flag indicating whether to overwrite existing annotated
                file.

        Returns:
            report: dictionary of metadata about the data and process

        Raises:
            Exception: on failure to open input or output data.
            Exception: on unexpected failure to read or write data.
        """
        start = time.perf_counter()
        self._log.log(
            f"Start Annotator.annotate_dwca_records: {time.asctime()}",
            refname=self.__class__.__name__)

        out_filename = BisonNameOp.get_process_outfilename(
            dwc_filename, outpath=output_path, step_or_process=LMBISON_PROCESS.ANNOTATE)
        report = {
            REPORT.INFILE: dwc_filename,
        }
        if os.path.exists(out_filename) and overwrite is False:
            msg = f"File {out_filename} exists and overwrite = False"
            report[REPORT.MESSAGE] = msg
            self._log.log(msg, refname=self.__class__.__name__)

        else:
            self.initialize_occurrences_io(dwc_filename, out_filename)
            self._log.log(
                f"Annotate {dwc_filename} to {out_filename}",
                refname=self.__class__.__name__)
            summary = {REPORT.ANNOTATE_FAIL: []}
            try:
                # iterate over DwC records
                dwcrec = self._dwcdata.get_record()

                # Only append additional values to records that pass the filter tests.
                while dwcrec is not None:
                    # Skip if the record is filtered out (rank above species)
                    taxkeys = self._filter_find_taxon_keys(dwcrec)
                    if len(taxkeys) > 0:
                        # Annotate and write
                        self._annotate_one_record(dwcrec, taxkeys)
                        try:
                            self._csv_writer.writerow(dwcrec)
                        except Exception as e:
                            self._log.log(
                                f"Error {e} record, gbifID {dwcrec[GBIF.ID_FLD]}",
                                refname=self.__class__.__name__, log_level=ERROR)
                            summary[REPORT.ANNOTATE_FAIL].append(dwcrec[GBIF.ID_FLD])

                    if (self._dwcdata.recno % LOG.INTERVAL) == 0:
                        self._log.log(
                            f"*** Record number {self._dwcdata.recno}, gbifID: "
                            f"{dwcrec[GBIF.ID_FLD]} ***",
                            refname=self.__class__.__name__)

                    # Get next
                    dwcrec = self._dwcdata.get_record()
            except Exception as e:
                raise Exception(
                    f"Unexpected error {e} reading {self._dwcdata.input_file} or "
                    + f"writing {out_filename}")
            else:
                end = time.perf_counter()
                report[REPORT.OUTFILE] = out_filename
                summary[REPORT.RANK_FAIL] = list(self.bad_ranks)
                summary[REPORT.RANK_FAIL_COUNT] = self.rank_filtered_records
                report[REPORT.SUMMARY] = summary

                self._log.log(
                    f"End Annotator.annotate_dwca_records: {time.asctime()}, {end - start} "
                    f"seconds elapsed", refname=self.__class__.__name__)
                self._log.log(
                    f"Filtered out {self.rank_filtered_records} records of {self.bad_ranks}",
                    refname=self.__class__.__name__)

        return report


# .............................................................................
def annotate_occurrence_file(
        dwc_filename, annotated_riis_filename, regions, geo_path, output_path,
        log_path):
    """Annotate GBIF records with census state and county, and RIIS key and assessment.

    Args:
        dwc_filename (str): full filename containing GBIF data for annotation.
        annotated_riis_filename (str): full filename with RIIS records annotated with
            gbif accepted taxa
        regions (list of common.constants.REGION): sequence of REGION members for
            intersection.  If None, use all.
        geo_path (str): Base directory containing geospatial region files
        output_path (str): destination directory for output annotated occurrence files.
        log_path (object): destination directory for logging processing messages.

    Returns:
        report (dict): metadata for the occurrence annotation data and process.

    Raises:
        FileNotFoundError: on missing input file
        OSError: on failure to write JSON report file
        IOError: on failure to write JSON report file
    """
    if not os.path.exists(dwc_filename):
        raise FileNotFoundError(dwc_filename)

    rpt_filename = BisonNameOp.get_process_report_filename(
        dwc_filename, output_path=output_path, step_or_process=LMBISON_PROCESS.ANNOTATE)
    logname, log_fname = BisonNameOp.get_process_logfilename(
        dwc_filename, log_path=log_path, step_or_process=LMBISON_PROCESS.ANNOTATE)
    logger = Logger(logname, log_filename=log_fname, log_console=False)

    ant = Annotator(
        logger, geo_path, annotated_riis_filename=annotated_riis_filename,
        regions=regions)
    report = ant.annotate_dwca_records(dwc_filename, output_path)

    # Write individual output report
    import json
    try:
        with open(rpt_filename, mode='wt') as out_file:
            json.dump(report, out_file, indent=4)
    except OSError:
        raise
    except IOError:
        raise
    logger.log(
        f"Wrote report file to {rpt_filename}", refname="annotate_occurrence_file")

    return report


# .............................................................................
def parallel_annotate(
        dwc_filenames, annotated_riis_filename, regions, geo_path, output_path, main_logger,
        overwrite):
    """Main method for parallel execution of DwC annotation script.

    Args:
        dwc_filenames (list): list of full filenames containing GBIF data for
            annotation.
        annotated_riis_filename (str): full filename with RIIS records annotated with
            gbif accepted taxa
        regions (list of common.constants.REGION): sequence of REGION members for
            intersection.  If None, use all.
        geo_path (str): input directory containing geospatial files for
            geo-referencing occurrence points.
        output_path (str): destination directory for output annotated occurrence files.
        main_logger (logger): logger for the process that calls this function,
            initiating subprocesses
        overwrite (bool): Flag indicating whether to overwrite existing annotated file.

    Returns:
        reports (dict): metadata for each subset file annotated by the process.
    """
    refname = "parallel_annotate"
    reports = []
    if output_path is None:
        output_path = os.path.dirname(dwc_filenames[0])
    log_path = main_logger.log_directory

    main_logger.log(
        f"Parallel Annotation Start Time : {time.asctime()}", refname=refname)

    files_to_annotate = []
    for fn in dwc_filenames:
        out_fname = BisonNameOp.get_process_outfilename(
            fn, outpath=output_path, step_or_process=LMBISON_PROCESS.ANNOTATE)
        if os.path.exists(out_fname) and overwrite is False:
            main_logger.log(f"Annotations exist in {out_fname}.", refname=refname)
        else:
            files_to_annotate.append(fn)

    with ProcessPoolExecutor(max_workers=available_cpu_count()-2) as executor:
        futures = []
        for dwc_fname in files_to_annotate:
            future = executor.submit(
                annotate_occurrence_file, dwc_fname, annotated_riis_filename,
                regions, geo_path, output_path, log_path)
            futures.append(future)

    # iterate over all submitted tasks and get results as they are available
    for future in as_completed(futures):
        # get the result for the next completed task
        reports.append(future.result())

    main_logger.log(
        f"Parallel Annotation End Time : {time.asctime()}", refname=refname)

    return reports


# .............................................................................
__all__ = [
    "annotate_occurrence_file",
    # "annotate_occurrence_file_update",
    "parallel_annotate",
    # "parallel_annotate_update"
]
