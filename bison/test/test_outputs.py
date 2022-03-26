"""Main script to execute all elements of the summarize-GBIF BISON workflow."""
# import csv
import glob
import os

from bison.common.aggregate import Aggregator
from bison.common.gbif import DwcData
from bison.common.constants import (
    AGGREGATOR_DELIMITER, DATA_PATH, NEW_GBIF_KEY_FLD, NEW_GBIF_SCINAME_FLD, NEW_RESOLVED_COUNTY, NEW_RESOLVED_STATE,
    NEW_RIIS_ASSESSMENT_FLD, SPECIES_KEY, ASSESS_KEY, LOCATION_KEY, COUNT_KEY)
from bison.tools.util import count_lines, get_csv_dict_reader, identify_chunk_files, parse_chunk_filename


# .............................................................................
def _get_species_name(occ_filename, riis_assessment):
    dwcdata = DwcData(occ_filename)
    dwcdata.open()

    # Find the species of the first record with riis_assessment
    rec = dwcdata.get_record()
    while rec is not None:
        if rec[NEW_RIIS_ASSESSMENT_FLD] == riis_assessment:
            spname = rec[NEW_GBIF_SCINAME_FLD]
            taxkey = rec[NEW_GBIF_KEY_FLD]
            county = rec[NEW_RESOLVED_COUNTY]
            state = rec[NEW_RESOLVED_STATE]
            break

    if spname is None:
        raise Exception(f"No {riis_assessment} records found in {occ_filename}")

    return spname, taxkey, county, state


# .............................................................................
def _get_location_count(region_summary_fname, species_key):
    # [SPECIES_KEY, COUNT_KEY, ASSESS_KEY]
    summary_count = None
    try:
        rdr, inf = get_csv_dict_reader(region_summary_fname, AGGREGATOR_DELIMITER)
        for rec in rdr:
            if rec[SPECIES_KEY] == species_key:
                summary_count = rec[COUNT_KEY]
                break
    except Exception as e:
        raise Exception(f"Unexpected open or read error {e} in {region_summary_fname}")
    finally:
        inf.close()

    if summary_count is None:
        raise Exception(f"Unable to find {species_key} in {region_summary_fname}")

    return summary_count

# .............................................................................
def count_species_for_location_for_riis(datapath, annotated_pattern, assessment="invasive"):
    """Test the number of matching annotated records against the values in location and assessment summary files.

    Args:
        datapath: full path to output directory.
        annotated_pattern: File pattern with wildcards for annotated input occurrence data files.
    """
    ann_pattern = os.path.join(datapath, annotated_pattern)
    # Files to count matching lines
    filenames = glob.glob(ann_pattern)

    # Get an invasive species occurrence from one annotated file
    spname, taxkey, county, state = _get_species_name(filenames[int(len(filenames) / 2)], assessment)

    # Count records of invasive species spname in county and state
    state_sp_count = count_lines(ann_pattern, [spname, assessment, state])
    county_sp_count = count_lines(ann_pattern, [spname, assessment, state, county])

    # Files to check values against line counts
    county_summary_fname = Aggregator.construct_location_summary_name(datapath, state, county=county)
    state_summary_fname = Aggregator.construct_location_summary_name(datapath, state)
    assess_summary_fname = Aggregator.construct_assessment_summary_name(datapath)

    # [SPECIES_KEY, COUNT_KEY, ASSESS_KEY]
    species_key = Aggregator.construct_compound_key(taxkey, spname)
    county_summary_count = _get_location_count(county_summary_fname, species_key)
    state_summary_count = _get_location_count(county_summary_fname, species_key)

    if state_sp_count == state_summary_count:
        print("Yay")
    else:
        print("boo")

    if county_sp_count == county_summary_count:
        print("Yay")
    else:
        print("boo")



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

    # Basename for constructing temporary files
    in_base_filename, ext = os.path.splitext(base_big_csv_filename)
    # Full path to input data
    big_csv_filename = os.path.join(DATA_PATH, args.big_csv_filename)

    base_pattern = f"{in_base_filename}"
    if do_split is True:
        base_pattern = f"{in_base_filename}_chunk"

    annotated_pattern = f"{base_pattern}*annotated{ext}"
    summary_annotated_pattern = f"{base_pattern}*annotated_summary{ext}"

    state_summary_pattern = Aggregator.construct_location_summary_name(DATA_PATH, "*")
    county_summary_pattern = Aggregator.construct_location_summary_name(DATA_PATH, "*", county="*")

    # Compare summary counts to line counts in annotated files
    count_species_for_location_for_riis(DATA_PATH, annotated_pattern)
