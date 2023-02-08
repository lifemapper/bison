"""Common classes for adding USGS RIIS info to GBIF occurrences."""
import logging
import os
from datetime import datetime
from multiprocessing import Pool, cpu_count

from bison.common.constants import (APPEND_TO_DWC, APPEND_TO_RIIS, ENCODING,
                                    GBIF, LOG, POINT_BUFFER_RANGE,
                                    US_CENSUS_COUNTY, US_STATES)
from bison.common.util import get_csv_dict_writer
from bison.process.geoindex import GeoException, GeoResolver
from bison.providers.gbif_data import DwcData
from bison.providers.riis_data import RIIS


# .............................................................................
class Annotator():
    """Class for adding USGS RIIS info to GBIF occurrences."""
    def __init__(
            self, gbif_occ_filename, geo_input_path, logger, riis_with_gbif_filename=None,
            riis=None):
        """Constructor.

        Args:
            gbif_occ_filename (str): full path of CSV occurrence file to annotate
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
        """
        datapath, _ = os.path.split(gbif_occ_filename)
        self._datapath = datapath
        self._csvfile = gbif_occ_filename
        # self._geopath = geo_input_path
        self._inf = None
        self._log = logger

        if riis is not None:
            self.riis = riis
            if self.riis.is_annotated is False:
                if riis_with_gbif_filename is None:
                    raise Exception(
                        "RIIS data does not contain GBIF accepted taxa, needed for "
                        "annotating GBIF occurrence data with RIIS Introduced and "
                        "Invasive determinations.  Provide a riis_with_gbif_filename "
                        "output file to initiate resolution of RIIS records.")
                else:
                    self.riis.resolve_riis_to_gbif_taxa(
                        riis_with_gbif_filename, overwrite=True)
        elif riis_with_gbif_filename is not None:
            self.riis = RIIS(riis_with_gbif_filename, logger, is_annotated=True)
            self.riis.read_riis()
        else:
            raise Exception(
                "Must provide either RIIS with annotations or riis_with_gbif_filename")

        # Must georeference points to add new, consistent state and county fields
        geofile = os.path.join(geo_input_path, US_CENSUS_COUNTY.FILE)
        self._geo_county = GeoResolver(
            geofile, US_CENSUS_COUNTY.GEO_BISON_MAP, self._log)

        # Input reader
        self._dwcdata = DwcData(self._csvfile, logger=logger)
        # Output writer
        self._csv_writer = None

        self._conus_states = []
        for k, v in US_STATES.items():
            if k not in ("Alaska", "Hawaii"):
                self._conus_states.extend([k, v])
        self._all_states = self._conus_states.copy()
        self._all_states.extend(["Alaska", "Hawaii", "AK", "HI"])
        self.bad_ranks = set()
        self.rank_filtered_records = 0

    # ...............................................
    @classmethod
    def construct_annotated_name(cls, csvfile):
        """Construct a full filename for the annotated version of csvfile.

        Args:
            csvfile (str): full filename used to construct an annotated filename
                for this data.

        Returns:
            outfname: output filename derived from the input GBIF DWC filename
        """
        pth, basefilename = os.path.split(csvfile)
        basename, ext = os.path.splitext(basefilename)
        try:
            rawidx = basename.index("_raw")
            basename = basename[:rawidx]
        except ValueError:
            pass
        newbasefilename = f"{basename}_annotated{ext}"
        outfname = os.path.join(pth, newbasefilename)
        return outfname

    # ...............................................
    @classmethod
    def construct_annotated_pattern(cls, csvfile):
        """Construct a full filename pattern matching the annotated file(s) of csvfile.

        Args:
            csvfile (str): full filename used to construct an annotated filename
                for this data.

        Returns:
            outfname: output filename derived from the input GBIF DWC filename
        """
        pth, basefilename = os.path.split(csvfile)
        basename, ext = os.path.splitext(basefilename)
        try:
            rawidx = basename.index("_raw")
            basename = basename[:rawidx]
        except ValueError:
            pass
        newbasefilename = f"{basename}_annotated{ext}"
        outfname = os.path.join(pth, newbasefilename)
        return outfname

    # ...............................................
    def _open_input_output(self, dwc_with_riis_filename):
        """Open the DwcData for reading and the csv_writer for writing.

        Also reads the first record and writes the header.

        Args:
            dwc_with_riis_filename: full filename of the output file

        Raises:
            Exception: on failure to open the DwcData csvreader.
            Exception: on failure to open the csv_writer.
        """
        # outfname = self.construct_annotated_name(self._csvfile)
        try:
            self._dwcdata.open()
        except Exception:
            raise

        header = self._dwcdata.fieldnames
        header.extend(
            [APPEND_TO_RIIS.RIIS_KEY, APPEND_TO_RIIS.RIIS_ASSESSMENT,
             APPEND_TO_DWC.RESOLVED_CTY, APPEND_TO_DWC.RESOLVED_ST])

        try:
            self._csv_writer, self._outf = get_csv_dict_writer(
                dwc_with_riis_filename, header, GBIF.DWCA_DELIMITER, fmode="w", encoding=ENCODING,
                overwrite=True)
        except Exception:
            raise Exception(f"Failed to open file or csv_writer for {dwc_with_riis_filename}")

    # ...............................................
    def close(self):
        """Close input datafiles and output file."""
        self._dwcdata.close()
        try:
            self._outf.close()
            self._csv_writer = None
        except AttributeError:
            pass

    # ...............................................
    @property
    def is_open(self):
        """Return true if input or output files are open.

        Returns:
            :type bool, True if a file is open, False if not
        """
        if ((self._inf is not None and not self._inf.closed)
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
    def annotate_one_record(self, dwcrec):
        """Append fields to GBIF record, then write to file.

        Args:
            dwcrec: one original GBIF DwC record

        Returns:
            dwcrec: one GBIF DwC record, if it is not filtered out, values are added
                to calculated fields: APPEND_TO_DWC, otherwise these fields are
                None.
        """
        gbif_id = dwcrec[GBIF.ID_FLD]
        if (self._dwcdata.recno % LOG.INTERVAL) == 0:
            self._log.info(
                f"*** Record number {self._dwcdata.recno}, gbifID: {gbif_id} ***")

        # Leave these fields None if the record is filtered out
        county = state = riis_assessment = riis_key = None
        filtered_taxkeys = self._filter_find_taxon_keys(dwcrec)

        # Only append additional values to records that pass the filter tests.
        if filtered_taxkeys:
            try:
                # Find county and state for these coords
                county, state = self._find_county_state(
                    dwcrec[GBIF.LON_FLD], dwcrec[GBIF.LAT_FLD],
                    buffer_vals=POINT_BUFFER_RANGE)
            except ValueError as e:
                self._log.error(f"Record gbifID: {gbif_id}: {e}")
            except GeoException as e:
                self._log.error(f"Record gbifID: {gbif_id}: {e}")

            region = "L48"
            if state in ("AK", "HI"):
                region = state

            # Find any RIIS records for these acceptedTaxonKeys and region.
            (riis_assessment,
             riis_key) = self.riis.get_assessment_for_gbif_taxonkeys_region(
                filtered_taxkeys, region)

        # Add county, state and RIIS assessment to record
        dwcrec[APPEND_TO_DWC.RESOLVED_CTY] = county
        dwcrec[APPEND_TO_DWC.RESOLVED_ST] = state
        dwcrec[APPEND_TO_DWC.RIIS_ASSESSMENT] = riis_assessment
        dwcrec[APPEND_TO_DWC.RIIS_KEY] = riis_key

        return dwcrec

    # ...............................................
    def annotate_dwca_records(self, dwc_with_geo_and_riis_filename):
        """Resolve and append state, county, RIIS assessment and key to GBIF records.

        Args:
            dwc_with_geo_and_riis_filename: full filename for writing DWC records with
                RIIS appended fields.

        Returns:
            report: dictionary of metadata about the data and process

        Raises:
            Exception: on failure to open input or output data.
            Exception: on unexpected failure to read or write data.
        """
        report = {
            "dwc_filename": self._csvfile,
            "dwc_with_geo_and_riis_filename": dwc_with_geo_and_riis_filename,
            "record_failed_gbifids": []
        }
        try:
            # Open the original DwC data file for read, annotated file for write.
            self._open_input_output(dwc_with_geo_and_riis_filename)
        except Exception:
            raise
        else:
            self._log.log(
                f"Annotating {self._csvfile} to create {dwc_with_geo_and_riis_filename}",
                refname=self.__class__.__name__)
            try:
                # iterate over DwC records
                dwcrec = self._dwcdata.get_record()
                while dwcrec is not None:
                    # Annotate
                    dwcrec_ann = self.annotate_one_record(dwcrec)
                    # Write
                    try:
                        self._csv_writer.writerow(dwcrec_ann)
                    except ValueError as e:
                        self._log.log(
                            f"ValueError {e} on record, gbifID {dwcrec[GBIF.ID_FLD]}",
                            refname=self.__class__.__name__, log_level=logging.ERROR)
                        report["record_failed_gbifids"].append(dwcrec[GBIF.ID_FLD])
                    except Exception as e:
                        self._log.log(
                            f"Unknown error {e} record, gbifID {dwcrec[GBIF.ID_FLD]}",
                            refname=self.__class__.__name__, log_level=logging.ERROR)
                        report["record_failed_gbifids"].append(dwcrec[GBIF.ID_FLD])
                    # Get next
                    dwcrec = self._dwcdata.get_record()
            except Exception as e:
                raise Exception(
                    f"Unexpected error {e} reading {self._dwcdata.input_file} or "
                    + f"writing {dwc_with_geo_and_riis_filename}")

        report["bad_ranks_filtered"] = self.bad_ranks
        report["records_filtered_by_rank"] = self.rank_filtered_records
        self._log.log(
            f"Annotate records filtered out {self.rank_filtered_records} " +
            f"records with {self.bad_ranks} ranks", refname=self.__class__.__name__)
        return report

    # ...............................................
    def _find_county_state(self, lon, lat, buffer_vals):
        county = state = None
        if None not in (lon, lat):
            # Intersect coordinates with county boundaries for state and county values
            try:
                fldvals, ogr_seconds = self._geo_county.find_enclosing_polygon(
                    lon, lat, buffer_vals=buffer_vals)
            except ValueError:
                raise
            except GeoException:
                raise
            if ogr_seconds > 0.75:
                self._log.log(
                    f"Rec {self._dwcdata.recno}; intersect point {lon}, {lat}; "
                    f"OGR time {ogr_seconds}", refname=self.__class__.__name__,
                    log_level=logging.DEBUG)
            county = fldvals[APPEND_TO_DWC.RESOLVED_CTY]
            state = fldvals[APPEND_TO_DWC.RESOLVED_ST]
        return county, state


# .............................................................................
def annotate_occurrence_file(
        dwc_filename, riis_with_gbif_taxa_filename, geo_input_path,
        dwc_with_geo_and_riis_filename, logger):
    """Annotate GBIF records with census state and county, and RIIS key and assessment.

    Args:
        dwc_filename (str): full filename containing GBIF data for annotation.
        riis_with_gbif_taxa_filename (str): filename containing RIIS data annotated with GBIF
            accepted taxon name and ID.
        dwc_with_geo_and_riis_filename: fullpath to GBIF data annotated with state, county,
            RIIS assessment, and RIIS key.
        logger (object): logger for saving relevant processing messages

    Returns:
        report (dict): dictionary of metadata about the data and process.

    Raises:
        FileNotFoundError: on missing input file
    """
    refname = "annotate_occurrence_file"
    if not os.path.exists(dwc_filename):
        raise FileNotFoundError(dwc_filename)
    report = {
        "riis_with_gbif_taxa_filename": riis_with_gbif_taxa_filename,
    }

    datapath, basefname = os.path.split(dwc_filename)
    logger.log(f"Submit {basefname} for annotation", refname=refname)
    logger.log("Start Time : {}".format(datetime.now()), refname=refname)

    ant = Annotator(
        dwc_filename, geo_input_path, logger, riis_with_gbif_filename=riis_with_gbif_taxa_filename)
    process_rpt = ant.annotate_dwca_records(geo_input_path, dwc_with_geo_and_riis_filename)
    report.update(process_rpt)

    logger.log("End Time : {}".format(datetime.now()), refname=refname)
    return report


# .............................................................................
def parallel_annotate(input_filenames, main_logger):
    """Main method for parallel execution of DwC annotation script.

    Args:
        input_filenames (list): list of full filenames containing GBIF data for
            annotation.
        main_logger (logger): logger for the process that calls this function,
            initiating subprocesses

    Returns:
        annotated_dwc_fnames (list): list of full output filenames
    """
    refname = "parallel_annotate"
    inputs = []
    # Process only needed files
    for in_csv in input_filenames:
        out_csv = Annotator.construct_annotated_name(in_csv)
        if os.path.exists(out_csv):
            main_logger.log(
                f"Annotations exist in {out_csv}, moving on.", refname=refname)
        else:
            inputs.append((in_csv, main_logger.log_directory))

    main_logger.log(
        "Parallel Annotation Start Time : {}".format(datetime.now()), refname=refname)
    # Do not use all CPUs
    pool = Pool(cpu_count() - 2)
    # Map input files asynchronously onto function
    # map_result = pool.map_async(annotate_occurrence_file, inputs)
    map_result = pool.starmap_async(annotate_occurrence_file, inputs)
    # Wait for results
    map_result.wait()
    annotated_dwc_fnames = map_result.get()
    main_logger.log(
        "Parallel Annotation End Time : {}".format(datetime.now()), refname=refname)

    return annotated_dwc_fnames
