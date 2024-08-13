AWS workflow
#############################

Get ancillary data
===================

* Census data at
  https://www.census.gov/geographies/mapping-files/time-series/geo/cartographic-boundary.html

  * Use 1:500,000 (or finer) where available
  * Use most current year (currently 2023)
  * US County Boundaries, 1:500,000, cb_2023_us_county_500k.shp
  * American Indian/Alaska Native Areas/Hawaiian Home Lands, AIANNH, 1:500,000,
    cb_2023_us_aiannh_500k.zip



Subset GBIF data
===================

The scripts for the following steps are in aws/redshift:

1. **subset_gbif.sql**: Load a subset of the latest GBIF data into Redshift

   * If doing in the Query Editor, choose the `bison` workgroup/namespace, and the
     `dev` database in the top left section of the interface.
   * Mount GBIF Open Data Registry on S3 to Redshift in an external schema
     (redshift_spectrum) in the dev database. (21sec)
   * Subset to US, with coordinates, Rank is Species or below, limited BasisOfRecord,
     with minimal fields (may rejoin other fields after Redshift steps) to create new
     table in public schema in dev database.  Subsets from 3 billion records to
     1 billion, 1.1min
   * Output is in the `bison` workgroup/namespace, in the `dev` database, `public`
     and named like bison_YYYY_MM_DD, with the date string indicating the most recent
     GBIF data on Amazon ODR.


Load ancillary data
===================
