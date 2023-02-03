"""Common classes for adding USGS RIIS info to GBIF occurrences."""
import logging
import os
from datetime import datetime
from multiprocessing import Pool, cpu_count

from bison.common.constants import (DATA_PATH, ENCODING, GBIF, LOG,
                                    NEW_RESOLVED_COUNTY, NEW_RESOLVED_STATE,
                                    NEW_RIIS_ASSESSMENT_FLD, NEW_RIIS_KEY_FLD,
                                    POINT_BUFFER_RANGE, RIIS, US_CENSUS_COUNTY,
                                    US_STATES)
from bison.common.log import Logger
from bison.common.util import get_csv_dict_writer
from bison.process.geoindex import GeoException, GeoResolver
from bison.providers.gbif_data import DwcData
from bison.providers.riis_data import NNSL


# .............................................................................
class Annotator():
    """Class for adding USGS RIIS info to GBIF occurrences."""
    def __init__(
            self, gbif_occ_filename, logger, annotated_riis_filename=None, nnsl=None):
        """Constructor.

        Args:
            gbif_occ_filename (str): full path of CSV occurrence file to annotate
            logger (object): logger for saving relevant processing messages
            annotated_riis_filename (str): full filename of RIIS data annotated with
                GBIF accepted taxon names.
            nnsl (bison.common.riis.NNSL): object containing USGS RIIS data for
                annotating records

        Raises:
            Exception: on neither annotated_riis_filename nor NNSL provided
        """
        datapath, _ = os.path.split(gbif_occ_filename)
        self._datapath = datapath
        self._csvfile = gbif_occ_filename
        self._inf = None
        self._log = logger

        if nnsl is not None:
            self.nnsl = nnsl
        elif annotated_riis_filename is not None:
            self.nnsl = NNSL(annotated_riis_filename, logger, is_annotated=True)
            self.nnsl.read_riis()
        else:
            raise Exception("Must provide either NNSL or annotated_riis_filename")

        # Must georeference points to add new, consistent state and county fields
        geofile = os.path.join(US_CENSUS_COUNTY.PATH, US_CENSUS_COUNTY.FILE)
        self._geo_county = GeoResolver(
            geofile, US_CENSUS_COUNTY.CENSUS_BISON_MAP, self._log)

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
    def _open_input_output(self):
        """Open the DwcData for reading and the csv_writer for writing.

        Also reads the first record and writes the header.

        Returns:
            outfname: full filename of the output file

        Raises:
            Exception: on failure to open the DwcData csvreader.
            Exception: on failure to open the csv_writer.
        """
        outfname = self.construct_annotated_name(self._csvfile)
        try:
            self._dwcdata.open()
        except Exception:
            raise

        header = self._dwcdata.fieldnames
        header.extend(
            [NEW_RIIS_KEY_FLD, NEW_RIIS_ASSESSMENT_FLD,
             NEW_RESOLVED_COUNTY, NEW_RESOLVED_STATE])

        try:
            self._csv_writer, self._outf = get_csv_dict_writer(
                outfname, header, GBIF.DWCA_DELIMITER, fmode="w", encoding=ENCODING,
                overwrite=True)
        except Exception:
            raise Exception(f"Failed to open file or csv_writer for {outfname}")

        return outfname

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
                to calculated fields: NEW_RESOLVED_COUNTY, NEW_RESOLVED_STATE,
                NEW_RIIS_ASSESSMENT_FLD, NEW_RIIS_KEY_FLD, otherwise these fields are
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
             riis_key) = self.nnsl.get_assessment_for_gbif_taxonkeys_region(
                filtered_taxkeys, region)

        # Add county, state and RIIS assessment to record
        dwcrec[NEW_RESOLVED_COUNTY] = county
        dwcrec[NEW_RESOLVED_STATE] = state
        dwcrec[NEW_RIIS_ASSESSMENT_FLD] = riis_assessment
        dwcrec[NEW_RIIS_KEY_FLD] = riis_key

        return dwcrec

    # ...............................................
    def annotate_dwca_records(self):
        """Resolve and append state, county, RIIS assessment and key to GBIF records.

        Returns:
            self.annotated_dwc_fname: full filename of the GBIF DWC records with
                appended fields.

        Raises:
            Exception: on failure to open input or output data.
            Exception: on unexpected failure to read or write data.
        """
        try:
            # Open the original DwC data file for read, annotated file for write.
            annotated_dwc_fname = self._open_input_output()
        except Exception:
            raise
        else:
            self._log.log(
                f"Annotating {self._csvfile} to create {annotated_dwc_fname}",
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
                    except Exception as e:
                        self._log.log(
                            f"Unknown error {e} record, gbifID {dwcrec[GBIF.ID_FLD]}",
                            refname=self.__class__.__name__, log_level=logging.ERROR)
                    # Get next
                    dwcrec = self._dwcdata.get_record()
            except Exception as e:
                raise Exception(
                    f"Unexpected error {e} reading {self._dwcdata.input_file} or "
                    + f"writing {annotated_dwc_fname}")

        self._log.log(
            f"Annotate records filtered out {self.rank_filtered_records} " +
            f"records with {self.bad_ranks} ranks", refname=self.__class__.__name__)

        return annotated_dwc_fname

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
            county = fldvals[NEW_RESOLVED_COUNTY]
            state = fldvals[NEW_RESOLVED_STATE]
        return county, state


# .............................................................................
def annotate_occurrence_file(gbif_occ_filename, log_directory):
    """Annotate GBIF records with census state and county, and RIIS key and assessment.

    Args:
        gbif_occ_filename (str): full filename containing GBIF data for annotation.
        log_directory (str): destination directory for logfile

    Returns:
        annotated_dwc_fname: fullpath to GBIF data annotated with state, county,
            RIIS assessment, and RIIS key.
        log_directory (str): directory path for output logfile

    Raises:
        FileNotFoundError: on missing input file
    """
    refname = "annotate_occurrence_file"
    if not os.path.exists(gbif_occ_filename):
        raise FileNotFoundError(gbif_occ_filename)

    datapath, basefname = os.path.split(gbif_occ_filename)
    logname = f"annotate_{basefname}"
    log_fname = os.path.join(log_directory, logname)
    logger = Logger(logname, log_fname, log_console=False)
    logger.log(f"Submit {basefname} for annotation", refname=refname)

    orig_riis_filename = os.path.join(DATA_PATH, RIIS.SPECIES_GEO_FNAME)
    nnsl = NNSL(orig_riis_filename, logger, is_annotated=False)
    nnsl.read_riis()

    logger.log("Start Time : {}".format(datetime.now()), refname=refname)
    ant = Annotator(gbif_occ_filename, logger, annotated_riis_filename=None, nnsl=nnsl)
    annotated_dwc_fname = ant.annotate_dwca_records()
    logger.log("End Time : {}".format(datetime.now()), refname=refname)
    return annotated_dwc_fname


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
