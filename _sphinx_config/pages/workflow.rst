AWS workflow
#############################

Get ancillary data
===================

* Census data at
  https://www.census.gov/geographies/mapping-files/time-series/geo/cartographic-boundary.html

  * Use 1:500,000 (or finer) where available
  * Use most current year (currently 2023)
  * All data is in Geographic coordinate reference system (EPSG:4326), which
    matches the coordinates in GBIF records
  * US County Boundaries, 1:500,000, cb_2023_us_county_500k.shp
  * American Indian/Alaska Native Areas/Hawaiian Home Lands, AIANNH, 1:500,000,
    cb_2023_us_aiannh_500k.zip

* Protected Areas Database (PAD) at
  https://www.usgs.gov/programs/gap-analysis-project/science/pad-us-data-download

  * PAD-US Vector Analysis Files
    These are "flattened" though spatial analysis prioritized by GAP Status Code
    (ie GAP 1 > GAP 2 > GAP > 3 > GAP 4), these are found on bottom of page
  * The Vector data is an ESRI geodatabase, accessible through GDAL/OGR using the
    OpenFileGDB driver.
  * Data is in EPSG:102039 - USA Contiguous Albers Equal Area Conic - USGS Version
  * Prepare the data for AWS

    * Download the vector file PADUS4_0VectorAnalysis_GAP_PADUS_Only_ClipCENSUS.zip
      for Alaska, Hawaii, continental US.
    * Unzip locally to PADUS4_0VectorAnalysis_GAP_PADUS_Only_ClipCENSUS.gdb, a
      directory containing a geodatabase.

Load ancillary data
===================

* Census data:

  * upload all files associated with a shapefile to S3
  * Look at the metadata using ogrinfo.  The `-al` switch indicates all layers,
    `-so` suppresses printing all features::

    ogrinfo -al -so -geom=SUMMARY cb_2023_us_aiannh_500k.shp
    ogrinfo -al -so -geom=SUMMARY cb_2023_us_county_500k.shp

  * Check and modify fields in the appropriate `CREATE TABLE` command in the
    aws/redshift/load_ancillary_data.sql script.
  * Run the `CREATE TABLE` command to create an empty table in Redshift,
    and the following `COPY` command to load the data into Redshift.

* PAD data:

  * Look at the metadata using ogrinfo::

      ogrinfo -al -so -geom=SUMMARY PADUS4_0VectorAnalysis_GAP_PADUS_Only_ClipCENSUS.gdb

  * Subset the geodatabase into shapefile, each with a GAP status of 1 or 2::

    ogr2ogr \
        -of "ESRI Shapefile" \
        -progress \
        -skipfailures \
        -where "GAP_Sts = '1'" \
        pad_4.0_gap1.shp  PADUS4_0VectorAnalysis_GAP_PADUS_Only_ClipCENSUS.gdb  \
        -nlt polygon \
        -lco ENCODING=UTF-8

        -where "GAP_Sts = '1' OR GAP_Sts = '2'" \
        -select SHAPE,OBJECTID,Mang_Type,Mang_Name,Loc_Ds,Unit_Nm,GAP_Sts,GIS_Acres \

  * Reproject each shapefile to EPSG:4326::

    ogr2ogr \
        -of "ESRI Shapefile" \
        -t_srs "EPSG:4326" \
        pad_4.0_gap1_4326.shp  pad_4.0_gap1.shp \
        -lco ENCODING=UTF-8

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
     schmema, and named like bison_YYYY_MM_DD, with the date string indicating the most
     recent GBIF data on Amazon ODR.

Troubleshooting
===================

PAD
-----

Still 3 errors with COPY command into Redshift::

    On line 1, column `objectid1`: "Invalid digit, Value 'E', Pos 10, Type: Integer"
    On line 31284: "Compass I/O exception: Invalid hexadecimal character(s) found"
    On line 41539, column `shape`:
      "Geometry size: 1089416 is larger than maximum supported size: 1048447"

