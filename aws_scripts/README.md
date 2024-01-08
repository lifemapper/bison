# 2024 AWS Data Processing

## Overview

1. **rs_mount_s3_data.sql**: Load latest GBIF data into Redshift

   * Use GBIF Open Data Registry on S3
   * Subset to US, with coordinates, Rank is Species or below, limited BasisOfRecord
   * Subset to minimum fields (will join other fields after Redshift steps)

1. **rs_load_ref_data.sql**: Load reference data into Redshift

   * use annotated RIIS (TODO: script for this step)
   * US census data: state/county, AIANNH
   * Protected Areas Database.  Data files split by Dept of Interior region and by
     state failed locally and would not load into AWS RDS.  Try these "flattened"
     versions next time:

    ```text
      In terms of PAD-US v 3.0, I'd recommend the following options, if decided to
      revisit: PAD-US 3.0 Vector/Raster Analysis Files
          https://www.sciencebase.gov/catalog/item/6196b9ffd34eb622f691aca7
          https://www.sciencebase.gov/catalog/item/6196bc01d34eb622f691acb5
      These are "flattened" though spatial analysis prioritized by GAP Status Code
      (ie GAP 1 > GAP 2 > GAP > 3 > GAP 4), these are found on bottom of page
      https://www.usgs.gov/programs/gap-analysis-project/science/pad-us-data-download
      ```

1. **rs_intersect_append.sql**: Annotate all GBIF records with fields from geospatial
   data and RIIS data, then export:

   * Intersect and annotate with:
     * State/County
     * AIANNH lands
     * PAD areas (later)
   * Add RIIS region (AK, HI, L48)
   * Lookup RIIS region/GBIF taxon key combo, annotate matching with
     introduced or invasive, annotate non-matching with presumed_native
   * Write records to S3

1. **rs_aggregate_export.sql**: Summarize records by each value for each region, then
   write files

   * one file of counts and percentages of introduced/invasive/presumed_native for each
     value of State_County, State, AIANNH, PAD (later)
   * one file per value of State_County, State, AIANNH, PAD with species lists and counts




## GBIF Input

GBIF exports all records to the Open Data Registry on Amazon, one copy in each region,
on the first of every month.  The records contain a subset of DwC fields.

## TODO

* Script workflow to execute on GBIF data update
