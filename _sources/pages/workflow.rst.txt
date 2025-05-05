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

* USGS Registry of Introduced and Invasive Species

  * Current data, version 2, is from 2022:

    * https://www.sciencebase.gov/catalog/item/62d59ae5d34e87fffb2dda99
    * https://www.invasivespeciesinfo.gov/resource/11555
    * Simpson, A., Fuller, P., Faccenda, K., Evenhuis, N., Matsunaga, J., and
      Bowser, M., 2022, United States Register of Introduced and Invasive Species
      (US-RIIS) (ver. 2.0, November 2022): U.S. Geological Survey data release,
      https://doi.org/10.5066/P9KFFTOD.
    * Data files:
      * USRIISv2_MasterList.csv: contains a record for any species determined to be
        Introduced or Invasive for a US region.  Regions are limited to Alaska, Hawaii,
        and the Continental US (AK, HI, L48). A species may be designated as Introduced or
        Invasive for one or more of these regions, with a record for each.
      * USRIISv2_AuthorityReferences.csv: contains citations for the RIIS designations
        in the MasterList.
      * US-RIISv2_DataDictionary.csv: contains full descriptions of the data in each
        field in the MasterList.

    * Categories in establishmentMeans field are:

      * introduced (alien, exotic, non-native, nonindigenous)
      * introduced: assisted colonization
      * introduced (alien, exotic, non-native, nonindigenous); introduced: assisted colonization

    * Categories in degreeOfEstablishment field are:

      * established (category C3)
      * invasive (category D2)
      * widespread invasive (category E)

Annotate RIIS Data with GBIF Accepted Taxa
==========================================

Run bison/tools/annotate_riis.py to annotate the file with the GBIF taxonKey and
scientificName for the currently accepted name for each record.  The annotated file
will be created in the same directory, and have the basename appended with
"_annotated_yyyy_mm_dd.csv" where yyyy_mm_dd is the first day of the current month.
This ensures that the RIIS data and the current GBIF data that have the same date
appended and taxonomic resolutions are current.

Resolve RIIS data
=======================

Add GBIF accepted taxa to RIIS data records of RIIS introduced/invasive status for
species by region (AK, HI, L48).  This provides a field to match on, as all GBIF records
include the accepted taxon.

This step is initiated by an EventBridge Schedule, and:

* Creates and launches an EC2 instance
* The "userdata" in the EC2 instance runs a script which:

  * pulls a Docker image containing BISON code from the Specify Docker Hub
  * starts the Docker instance
  * initiates the "annotate_riis" command.
  * when the annotation is complete, it uploads the annotated RIIS file to S3

* TODO: The update of the annotated RIIS file on S3 is an event which triggers:

  * stop the EC2 instance.
  * initiate the next step.

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

* Annotated RIIS data

  * upload the annotated RIIS CSV file to S3.


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

* Look at the metadata using ogrinfo::

      ogrinfo -al -so -geom=SUMMARY PADUS4_0VectorAnalysis_GAP_PADUS_Only_ClipCENSUS.gdb

* Subset the geodatabase into shapefile, each with a GAP status of 1 or 2::

    ogr2ogr \
        -of "ESRI Shapefile" \
        -progress \
        -skipfailures \
        -where "GAP_Sts = '1'" \
        pad_4.0_gap1b.shp  PADUS4_0VectorAnalysis_GAP_PADUS_Only_ClipCENSUS.gdb  \
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

* Create an empty table in Redshift::

    CREATE TABLE pad1 (
       SHAPE     GEOMETRY,
       OBJECTID  INTEGER,
       Mang_Type VARCHAR(max),
       Mang_Name VARCHAR(max),
       Loc_Ds    VARCHAR(max),
       Unit_Nm   VARCHAR(max),
       GAP_Sts   VARCHAR(max),
       GIS_Acres VARCHAR(max)
    );


* Fill table from S3::

    COPY pad1 FROM 's3://bison-321942852011-us-east-1/input_data/pad_4.0_gap1_4326.shp'
    FORMAT SHAPEFILE
    SIMPLIFY AUTO
    IAM_role DEFAULT;

* Always error, even when reducing the number of records or using all fields::

    Compass I/O exception: Invalid hexadecimal character(s) found
