"""Main script to execute all elements of the summarize-GBIF BISON workflow."""
import glob
import os

from bison.common.aggregate import Aggregator, RIIS_Counts
from bison.common.gbif import DwcData
from bison.common.constants import (
    ASSESS_VALUES, DATA_PATH, GBIF, NEW_RESOLVED_COUNTY,
    NEW_RESOLVED_STATE, NEW_RIIS_ASSESSMENT_FLD, SPECIES_KEY, STATE_KEY, COUNTY_KEY, ASSESS_KEY, COUNT_KEY,
    INTRODUCED_SPECIES, INVASIVE_SPECIES, NATIVE_SPECIES, INTRODUCED_OCCS, INVASIVE_OCCS, NATIVE_OCCS)
from bison.tools.util import get_csv_dict_reader, get_logger


# # .............................................................................
# class Counter():
#     """Class for comparing counts for a RIIS assessment."""
#     def __init__(self, csv_filename, do_split=True, logger=None):
#         if not os.path.exists(csv_filename):
#             raise FileNotFoundError(f"File {csv_filename} does not exist")
#
#         # Basename for constructing temporary files
#         in_base_filename, ext = os.path.splitext(csv_filename)
#
#         base_pattern = f"{in_base_filename}"
#         if do_split is True:
#             base_pattern = f"{in_base_filename}_chunk"
#
#         annotated_pattern = f"{base_pattern}*annotated{ext}"
#         summary_annotated_pattern = f"{base_pattern}*annotated_summary{ext}"
#
#         self._datapath, _ = os.path.split(csv_filename)
#         self._csvfile = csv_filename
#         if logger is None:
#             logger = get_logger(self._datapath)
#         self._log = logger


# .............................................................................
def _get_random_species(occ_filename, riis_assessment):
    # Get one species name and county-state with riis_assessment from annotated occurrences file
    spname = None
    dwcdata = DwcData(occ_filename)
    try:
        dwcdata.open()

        # Find the species of the first record with riis_assessment
        rec = dwcdata.get_record()
        while rec is not None:
            if rec[NEW_RIIS_ASSESSMENT_FLD] == riis_assessment:
                spname = rec[GBIF.ACC_NAME_FLD]
                taxkey = rec[GBIF.ACC_TAXON_FLD]
                county = rec[NEW_RESOLVED_COUNTY]
                state = rec[NEW_RESOLVED_STATE]
                print(f"Found {spname} on line {dwcdata.recno}")
                break
            rec = dwcdata.get_record()
    except Exception as e:
        raise Exception(f"Unknown exception {e} on file {occ_filename}")
    finally:
        dwcdata.close()

    if spname is None:
        raise Exception(f"No {riis_assessment} records found in {occ_filename}")

    return spname, taxkey, county, state


# .............................................................................
def _count_annotated_records(filename_pattern, spname, taxkey, state, county, logger=None):
    filenames = glob.glob(filename_pattern)
    state_counts = RIIS_Counts(is_group=False, logger=logger)
    cty_counts = RIIS_Counts(is_group=False, logger=logger)
    for fn in filenames:
        try:
            dwcdata = DwcData(fn)
            dwcdata.open()

            # Find the species of the first record with riis_assessment
            rec = dwcdata.get_record()
            while rec is not None:
                if rec[GBIF.ACC_TAXON_FLD] == taxkey and rec[NEW_RESOLVED_STATE] == state:
                    assess = rec[NEW_RIIS_ASSESSMENT_FLD]
                    # Add to state count
                    state_counts.add_to(assess, value=1)
                    if rec[NEW_RESOLVED_COUNTY] == county:
                        # Add to county count
                        cty_counts.add_to(assess, value=1)
                rec = dwcdata.get_record()
        except Exception as e:
            raise Exception(f"Unknown exception {e} on file {fn}")
        finally:
            dwcdata.close()
            dwcdata = None

    logger.info(f"Counted occurrences of {spname} in {state} and {county} in {len(filenames)} annotated files")

    return state_counts, cty_counts


# .............................................................................
def _get_location_count(region_summary_fname, species_name, logger=None):
    # Get counts for all assessments of species_key in this region summary file
    loc_occ_counts = RIIS_Counts(is_group=False, logger=logger)
    try:
        rdr, inf = get_csv_dict_reader(region_summary_fname, GBIF.DWCA_DELIMITER)
        for rec in rdr:
            if rec[SPECIES_KEY] == species_name:
                rass = rec[ASSESS_KEY]
                count = int(rec[COUNT_KEY])
                if rass == "introduced":
                    loc_occ_counts.introduced = count
                elif rass == "invasive":
                    loc_occ_counts.invasive = count
                elif rass == "presumed_native":
                    loc_occ_counts.presumed_native = count
                else:
                    raise Exception(f"Unknown record field {rass}.")
                break
    except Exception as e:
        raise Exception(f"Unexpected open or read error {e} in {region_summary_fname}")
    finally:
        inf.close()

    logger.info(f"Read occurrence summary for {species_name} in {region_summary_fname} summary file")

    return loc_occ_counts


# .............................................................................
def _get_assess_summary(assess_summary_fname, state, county=None, logger=None):
    species_counts = occ_counts = None
    try:
        rdr, inf = get_csv_dict_reader(assess_summary_fname, GBIF.DWCA_DELIMITER)
        for rec in rdr:
            if rec[STATE_KEY] == state:
                if county is None:
                    if not rec[COUNTY_KEY]:
                        species_counts = RIIS_Counts(
                            introduced=rec[INTRODUCED_SPECIES], invasive=rec[INVASIVE_SPECIES],
                            presumed_native=rec[NATIVE_SPECIES], is_group=True, logger=logger)
                        occ_counts = RIIS_Counts(
                            introduced=rec[INTRODUCED_OCCS], invasive=rec[INVASIVE_OCCS],
                            presumed_native=rec[NATIVE_OCCS], is_group=False, logger=logger)
                        break
                elif rec[COUNTY_KEY] == county:
                    species_counts = RIIS_Counts(
                        introduced=rec[INTRODUCED_SPECIES], invasive=rec[INVASIVE_SPECIES],
                        presumed_native=rec[NATIVE_SPECIES], is_group=True, logger=logger)
                    occ_counts = RIIS_Counts(
                        introduced=rec[INTRODUCED_OCCS], invasive=rec[INVASIVE_OCCS],
                        presumed_native=rec[NATIVE_OCCS], is_group=False, logger=logger)
                    break
    except Exception as e:
        raise Exception(f"Unexpected error {e} in opening or reading {assess_summary_fname}")
    return (species_counts, occ_counts)


# .............................................................................
def compare_counts(datapath, annotated_pattern, spname, taxkey, county, state, logger=None):
    """Test the number of matching annotated records against the values in location and assessment summary files.

    Args:
        datapath (str): full path to output directory.
        annotated_pattern (str): basefilename pattern for matching subset files
        spname (str): species name
        taxkey (str): GBIF accepted taxon key for species name
        county (str): county
        state (str): 2-character state code
        logger (object): logger for saving relevant processing messages
    """
    # Count matching lines in annotated records
    filename_pattern = os.path.join(datapath, annotated_pattern)
    state_ann_occ_counts, county_ann_occ_counts = _count_annotated_records(
        filename_pattern, spname, taxkey, state, county, logger=logger)

    # [SPECIES_KEY, COUNT_KEY, ASSESS_KEY]
    county_summary_fname = Aggregator.construct_location_summary_name(datapath, state, county=county)
    state_summary_fname = Aggregator.construct_location_summary_name(datapath, state)
    species_key = Aggregator.construct_compound_key(taxkey, spname)
    for assess in ASSESS_VALUES:
        # Counts from state and county summary files
        county_loc_occ_counts = _get_location_count(county_summary_fname, species_key, logger=logger)
        state_loc_occ_counts = _get_location_count(state_summary_fname, species_key, logger=logger)

    # Counts from RIIS assessment summary
    assess_summary_fname = Aggregator.construct_assessment_summary_name(datapath)
    (st_species_counts, st_occ_counts) = _get_assess_summary(assess_summary_fname, state, logger=logger)
    (cty_species_counts, cty_occ_counts) = _get_assess_summary(assess_summary_fname, state, county=county, logger=logger)

    # Count annotated records vs location summaries
    # County
    if county_ann_occ_counts.equals(county_loc_occ_counts):
        logger.info("Success! Counts from annotated files match county location file summaries")
    else:
        logger.info("Fail! Counts from annotated files match county location file summaries")
    # State
    if state_ann_occ_counts.equals(state_loc_occ_counts):
        logger.info("Success! Counts from annotated files match state location file summaries")
    else:
        logger.info("Fail! Counts from annotated files match state location file summaries")

    # Count annotated records vs assessment summary
    # County
    if county_ann_occ_counts.equals(cty_occ_counts):
        logger.info("Success! Counts from annotated files match assessment summary")
    else:
        logger.info("Fail! Counts from annotated files match assessment summary")
    # State
    if state_ann_occ_counts.equals(st_occ_counts):
        logger.info("Success! Counts from annotated files match assessment summary")
    else:
        logger.info("Fail! Counts from annotated files match assessment summary")


# .............................................................................
def count_species_for_location_for_riis(datapath, annotated_pattern, logger=None):
    """Test the number of matching annotated records against the values in location and assessment summary files.

    Args:
        datapath: full path to output directory.
        annotated_pattern: File pattern with wildcards for annotated input occurrence data files.
        logger (object): logger for saving relevant processing messages
    """
    ann_pattern = os.path.join(datapath, annotated_pattern)
    # Files to count matching lines
    filenames = glob.glob(ann_pattern)

    # Get an invasive species occurrence from one annotated file
    spname, taxkey, county, state = _get_random_species(filenames[int(len(filenames) / 2)], "invasive")

    compare_counts(datapath, annotated_pattern, spname, taxkey, county, state, logger=logger)


# .............................................................................
# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    import argparse

    gbif_infile = os.path.join(DATA_PATH, "gbif_2022-03-16.csv")

    parser = argparse.ArgumentParser(
        description="Test outputs of summarized GBIF data with RIIS assessments.")
    parser.add_argument(
        "big_csv_filename", type=str, default=gbif_infile,
        help="The full path to GBIF input species occurrence data.")
    parser.add_argument(
        "--do-split", type=str, choices=("True", "False"), default="True",
        help="True to test subsetted/chunked files; False to process big_csv_filename directly.")

    args = parser.parse_args()
    base_big_csv_filename = args.big_csv_filename
    do_split = True if args.do_split.lower() in ("yes", "y", "true", "1") else False
    logger = get_logger(DATA_PATH, logname="main_test_outputs")

    # ...............................................
    # Test data
    # ...............................................
    base_big_csv_filename = "gbif_2022-03-16_100k.csv"
    # ...............................................

    # # Basename for constructing temporary files
    in_base_filename, ext = os.path.splitext(base_big_csv_filename)
    # Full path to input data
    big_csv_filename = os.path.join(DATA_PATH, args.big_csv_filename)

    tst = Counter(big_csv_filename, do_split=True, logger=logger)


    base_pattern = f"{in_base_filename}"
    if do_split is True:
        base_pattern = f"{in_base_filename}_chunk"

    annotated_pattern = f"{base_pattern}*annotated{ext}"
    summary_annotated_pattern = f"{base_pattern}*annotated_summary{ext}"

    state_summary_pattern = Aggregator.construct_location_summary_name(DATA_PATH, "*")
    county_summary_pattern = Aggregator.construct_location_summary_name(DATA_PATH, "*", county="*")

    # Compare summary counts to line counts in annotated files
    count_species_for_location_for_riis(DATA_PATH, annotated_pattern, logger=logger)
