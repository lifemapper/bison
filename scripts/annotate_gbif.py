"""Unfinished Glue Script to annotate GBIF records."""
import boto3
import os

from bison.common.constants import APPEND_TO_DWC, CENSUS_STATE_FLDNAME, GBIF, REGION
from bison.process.geoindex import GeoResolver
from bison.provider.riis_data import RIIS

project = "lmbison"
account_id = "321942852011"
region = "us-east-1"

raw_data_prefix = "data-ingestion"
subset_prefix = "subset"

GEO_PATH = ""
ANNOTATED_RIIS_FILE = ""


# .............................................................................
def get_riis_lookup():
    riis_lookup = RIIS(ANNOTATED_RIIS_FILE)
    riis_lookup.read_riis()
    return riis_lookup


# .............................................................................
def get_georesolvers():
    fieldmap = {}
    for region in (REGION.COUNTY, REGION.AIANNH, REGION.PAD):
        if region == REGION.PAD:
            pad_geo_subsets = {}
            state_file = REGION.get_state_files_from_pattern(GEO_PATH)
            for subset, full_fn in state_file.items():
                sub_geores = GeoResolver(
                    full_fn, REGION.PAD["map"], is_disjoint=REGION.PAD["is_disjoint"],
                    buffer_vals=REGION.PAD["buffer"])
                pad_geo_subsets[subset] = sub_geores
            fieldmap.update(sub_geores.fieldmap)
        else:
            filename = os.path.join(GEO_PATH, region["file"])
            geores = GeoResolver(
                filename, region["map"], is_disjoint=region["is_disjoint"],
                buffer_vals=region["buffer"])
            fieldmap.update(geores.fieldmap)
            if region == REGION.COUNTY:
                county_geo = geores
            elif region == REGION.AIANNH:
                aiannh_geo = geores
    return county_geo, aiannh_geo, pad_geo_subsets, fieldmap


# .............................................................................
def batch_handler(event, context):
    s3 = boto3.client("s3")
    glue = boto3.client("glue")
    county_geo, aiannh_geo, pad_geo_subsets, fieldmap = get_georesolvers()
    riis_lookup = get_riis_lookup()

    # TODO: How to read records?
    subset_gbif_data = []
    for record in subset_gbif_data:
        s3_object_key = event["s3_object_key"]
        rec = s3.get_object(
            Bucket=f"{subset_prefix}-{account_id}-{region}/{project}", Key=s3_object_key)
        compute_values(
            rec, county_geo, aiannh_geo, pad_geo_subsets, fieldmap, riis_lookup)
    # TODO: Fill TableName
    glue.update_record(TableName="", Record=rec)


# .............................................................................
def compute_values(
        rec, county_geo, aiannh_geo, pad_geo_subsets, fieldmap, riis_lookup):
    # s3_object_key = event["s3_object_key"]
    # rec = s3.get_object(
    #     Bucket=f"{subset_prefix}-{account_id}-{region}/{project}", Key=s3_object_key)

    # Read values from the record
    gbif_id = rec[GBIF.ID_FLD]
    lon = rec[GBIF.LON_FLD]
    lat = rec[GBIF.LAT_FLD]
    rank = rec[GBIF.RANK_FLD].lower()
    acc_taxon_key = rec[GBIF.ACC_TAXON_FLD]
    species_key = rec[GBIF.SPECIES_KEY_FLD]
    taxon_keys = filter_find_taxon_keys(rank, acc_taxon_key, species_key)

    # TODO: How to delete the record?  Write to different table?
    if taxon_keys:
        # Get state and county
        fldvals = compute_state_county(gbif_id, lon, lat, county_geo)
        state = fldvals[CENSUS_STATE_FLDNAME]

        # Get AIANNH
        fldvals.update(compute_aiannh(gbif_id, lon, lat, aiannh_geo))

        # Get PAD by state
        pad_geo = pad_geo_subsets[state]
        fldvals.update(compute_pad(gbif_id, lon, lat, pad_geo))

        # Get RIIS by state
        riis_assessment, riis_key = compute_riis(
            gbif_id, taxon_keys, state, riis_lookup)

        # Update all new values
        rec[APPEND_TO_DWC.RIIS_ASSESSMENT] = riis_assessment
        rec[APPEND_TO_DWC.RIIS_KEY] = riis_key
        for in_fld, bison_fld in fieldmap.items():
            rec[bison_fld] = fldvals[in_fld]


# .............................................................................
def compute_state_county(gbif_id, lon, lat, county_geo):
    fldvals = {}
    # Find enclosing region and its attributes
    try:
        fldval_list = county_geo.find_enclosing_polygon_attributes(lon, lat)
    except ValueError as e:
        print(f"Record gbifID: {gbif_id}: {e}")
    else:
        # These should only return values for one feature, take the first
        if fldval_list:
            fldvals = fldval_list[0]
    return fldvals


# .............................................................................
def compute_aiannh(gbif_id, lon, lat, aiannh_geo):
    fldvals = {}
    # Find enclosing region and its attributes
    try:
        fldval_list = aiannh_geo.find_enclosing_polygon_attributes(lon, lat)
    except ValueError as e:
        print(f"Record gbifID: {gbif_id}: {e}")
    else:
        # These should only return values for one feature, take the first
        if fldval_list:
            fldvals = fldval_list[0]
    return fldvals


# .............................................................................
def compute_pad(gbif_id, lon, lat, pad_geo):
    fldvals = {}
    try:
        fldval_list = pad_geo.find_enclosing_polygon_attributes(lon, lat)
    except ValueError as e:
        print(f"Record gbifID: {gbif_id}: {e}")
    else:
        if fldval_list:
            # May return many - get first feature with GAP status for most
            # protection (lowest val)
            fldvals = fldval_list[0]
            if len(fldval_list) > 1:
                for fv in fldval_list:
                    if (
                            fv[APPEND_TO_DWC.PAD_GAP_STATUS] <
                            fldvals[APPEND_TO_DWC.PAD_GAP_STATUS]
                    ):
                        fldvals = fv
    return fldvals


# .............................................................................
def compute_riis(gbif_id, taxon_keys, state, riis_lookup):
    assess = "presumed_native"
    recid = None
    if state is None or state == "":
        print(f"No state for gbifID: {gbif_id}")
    else:
        riis_region = "L48"
        if state in ("AK", "HI"):
            riis_region = state

        riis_recs = []
        for gbif_taxon_key in taxon_keys:
            riis_recs.extend(riis_lookup.get_riis_by_gbif_taxonkey(gbif_taxon_key))
        for riis in riis_recs:
            if region == riis.locality:
                assess = riis.assessment
                recid = riis.occurrence_id
        return assess, recid


# ...............................................
def filter_find_taxon_keys(rank, acc_taxon_key, species_key):
    """Returns acceptedTaxonKey for species and a lower rank key where determined.

    Args:
        rank: taxonomic rank, lowercase
        acc_taxon_key: gbif identifier for the accepted taxonomic name
        species_key: gbif identifier for the accepted species name

    Returns:
        taxkeys (list of int): if the record is Species rank or below, return the
            accepted species GBIF taxonKey and, if lower than species, the lower
            rank acceptedTaxonKey.
    """
    taxkeys = []
    # Identify whether this record is above Species rank
    # (exclude higher level determinations from annotation and summary)
    if rank in GBIF.ACCEPT_RANK_VALUES:
        # Find RIIS records for this acceptedTaxonKey.
        taxkeys.append(acc_taxon_key)
        if (rank != "species" and acc_taxon_key != species_key):
            # If acceptedTaxonKey is below species, also find RIIS records for species
            taxkeys.append(species_key)
    return taxkeys
