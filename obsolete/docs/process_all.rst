
.. highlight:: rest

Data Load processing for both BISON-Provider and GBIF data
============================================================
.. contents::

.. _GBIF Dataset Processing: docs/notes/process_gbif.rst
.. _BISON-Provider Dataset Processing: docs/notes/process_bisonprovider.rst


GBIF and BISON provider common processing
-------------------------------------------
To be done on BISON 48, either from GBIF or data providers,
process info at: https://my.usgs.gov/confluence/display/DEV/SAS+Development

Process in separate steps to fill field dependencies before they are needed.

Reference Data Preparation:
============================

* ITIS data

  * Get updated lookup table created by ITIS developers provided by Denver
  * Make sure kingdom and amb fields are included
  * When loading:

    * strip trailing spaces from ITIS kingdom values
    * strip external quotes from data

  * Rejected options:

    * Use API?  No, Solr query is fast, but REST query for common names is slow
    * Use downloaded database from https://www.itis.gov/downloads/index.html
      No, data is not as current as version provided by ITIS developers

* Establishment means

  * Get lookup table from Annie Simpson

* US Counties and Canada Provinces

  * Get US counties from Shayne and Canada from Denver
  * Merge US and Canada shapefiles
    * Compute the centroid coordinates of each input feature.
    * Add new fields for BISON data processing which compute/edit values from
      the input datasets to fill calculated_state_name,
      calculated_county_name, calculated_fips, and longitude and latitude
      coordinates if needed and possible.
    * field mapping:
      * Canada PRNAME to calculated_state_name, strip " Canada" from value
      * Canada CDNAME to calculated_county_name
      * Canada CDUID to calculated_fips
      * US STATE_FIPS + CNTY_FIPS = calculated_fips
    * Simplify the output polygon shapefile, replacing MULTIPOLYGON features
      with the component simple POLYGON features.  Replicate attribute values
      from the original polygon into the simple polygons.
    * Code creates LUT (by state/county or fips) for faster lookup.

* Marine EEZ

    * Simplify the marine EEZ shapefile into a shapefile with smaller, simpler
      polygons for intersection.

      * Replace MULTIPOLYGON features with the component simple POLYGON features.
        Replicate attribute values from the original polygon into the simple
        polygons.
      * Create a 2.5 degree global grid
      * Intersect simplified polygons with global grid for smaller, simpler
        polygons.  Split complex intersections from MULTIPOLYGON or
        GEOMETRYCOLLECTION into simple POLYGON

Step 1: Handle quotes, check coords, fill resource/provider
============================================================
* Ecape internal quotes and removing enclosing quotes
* Check longitude and negate if needed for US and Canada records
* Fill resource/provider fields from

  * current GBIF metadata merged with BISON tables (for GBIF data)
  * new or modified metadata from Jira tickets (for BISON provider data)

Step 2: GBIF only
=================
see `GBIF Data Processing`_


Step 3: Clean, lookup ITIS, Establishment Means, Centroid
==========================================================
* Cleanup/standardization

  * kingdom: If existing value in ITIS kingdoms, make sure it is capitalized.
    Overwrite if ITIS lookup succeeds.
  * quote characters: remove enclosing quotes from all field contents, and
    escape quote characters
  * field type: check the field type and length, discard values that cannot
    be coerced to the correct type, truncate string values that are too long

* ITIS lookup: fill ITIS values from clean_provided_scientific_name

  * Lookup TSN from clean_provided_scientific_name to fill fields:

    * itis_common_name
    * itis_tsn
    * valid_accepted_scientific_name
    * valid_accepted_tsn
    * kingdom
      * New: first just check capitalization, now rewrite existing value

* Establishment Means lookup

  * Lookup by ITIS TSN first, (NEW) by clean_provided_scientific_name second, to
    fill establishment_means field

* Centroid lookup

  * For records without coordinates:

    * Lookup values by provided_state + provided_county or provided_fips,
      if matching:

      * fill longitude/latitude with centroid coordinates
      * fill centroid field with 'county'
      * fill datum field with 'WGS84'

Step 4: Geospatial intersection
================================

* Use US/Canada merged, modified shapefiles prepared above.  Code creates a
  spatial index for faster lookup.

  * On records with longitude/latitude (including those filled with
    centroid coordinates)

    * Code creates a spatial index for geospatial intersection
    * Do point-in-polygon query on shapefiles to fill
      calculated_state_name, calculated_county_name, calculated_fips from
      US Counties.zip for US and Canada: https://my.usgs.gov/jira/browse/BISA-1143
    * New: compute calculated_state_fips from calculated_fips
    * Do point-in-polygon query on shapefiles to fill
      calculated_waterbody, mrgid from
      World_EEZ_v8_20140228_splitpolygons.zip, using attributes MRGID & EEZEEZ:
      https://my.usgs.gov/jira/browse/BISA-763
    * If point intersects with > 1 terrestrial or marine polygon, leave blank
    * If point intersects with terrestrial AND marine polygons, leave blank

  * On records with NO longitude/latitude, use geography lookup table for

    * if provided_state_name + provided_county_name, fill longitude/latitude
      with county centroid coordinates, fill centroid with "county"
    * if provided_fips, fill longitude/latitude
      with fips centroid coordinates, fill centroid with "county"



BISON 49 (48 database fields + hierarchy_string and amb for Solr)
-------------------------------------------------------------------
#. For GBIF data load, field values will be pulled from named GBIF field or
calculated according to rules laid out in `GBIF Data Processing`_.

#. For BISON provider data processing, the data should already be correctly
populated.  I will re-compute and fill *only* the values noted in "common processing":

   * itis_common_name, itis_tsn, hierarchy_string, amb, (ITIS lookup, establishment_means, coordinates (if record has missing coords
     and matching state/county/fips and centroid field != 'county')
   * calculated_state_name, calculated_county_name, calculated_fips,
     calculated_waterbody, mrgid.

#. clean_provided_scientific_name

   * Calc: 1) gbif name parser and scientificName OR
     2) gbif species api and taxonKey

#. itis_common_name

   * Calc: from ITIS lookup

#. itis_tsn

   * Calc: with ITIS lookup + clean_provided_scientific_name

#. hierarchy_string

   * Calc: from ITIS lookup

#. amb

   * Calc: from ITIS lookup

#. basis_of_record

   * Calc: gbif/dwc basisOfRecord + controlled vocabulary

#. occurrence_date

   * Calc: gbif/dwc eventDate - formatted to YYYY-MM-DD if full date, or YYYY

#. year

   * Calc: gbif/dwc year or pulled from occurrence_date calc

#. verbatim_event_date

   * gbif/dwc verbatimEventDate

#. provider

   * Calc: 'title' from provider lookup table (LUT).
     LUT combines provider table from BISON-Denver and
     GBIF organization API + organization key (from dataset metadata)

#. provider_url

   * Calc: 'homepage' or 'url' from provider lookup table (LUT).
     LUT combines provider table from BISON-Denver and
     GBIF organization API + organization key (from dataset metadata)

#. resource

   * Calc: 'title' from resource lookup table (LUT).
     LUT combines resource table from BISON Denver and
     GBIF dataset API + dataset key

#. resource_url

   * Calc: 'homepage' or 'url' from resource lookup table (LUT).
     LUT combines resource table from BISON Denver and
     GBIF dataset API + dataset key.  Remove record if
     provider UUID = BISON UUID and
     resource_url = https://bison.usgs.gov/ipt/resource?r=*

#. occurrence_url

   * gbif/dwc occurrenceID

#. catalog_number

   * gbif/dwc catalogNumber

#. collector

   * gbif/dwc recordedBy

#. collector_number

   * gbif/dwc recordNumber

#. valid_accepted_scientific_name

   * Calc: ITIS lookup

#. valid_accepted_tsn

   * Calc: ITIS lookup

#. provided_scientific_name

   * gbif/dwc scientificName (AMS: later, check verbatim file)

#. provided_tsn

   * gbif/dwc taxonID

#. latitude

   * first pass: gbif/dwc decimalLatitude if exist and valid
   * second pass if missing: Calc: Geo lookup from centroids of smallest
     enclosing polygon in provided shapefiles

#. longitude (DwC: decimalLongitude)

   * first pass: gbif/dwc decimalLongitude if exist and valid
   * second pass if missing: Calc: Geo lookup from centroids of smallest
     enclosing polygon in provided shapefiles

#. verbatim_elevation

   * gbif/dwc verbatimElevation

#. verbatim_depth

   * gbif/dwc verbatimDepth

#. calculated_county_name

   * Calc: Point-in-polygon terrestrial - coordinates + county polygons

#. calculated_fips

   * Calc: Point-in-polygon terrestrial - coordinates + fips polygons

#. calculated_state_name

   * Calc: Point-in-polygon terrestrial - coordinates + state polygons

#. centroid

   * Calc: populate if coordinates calculated from Geo lookup to polygon
   * Overwrite existing values in BISON-provided datasets *only* if it was
     previously georeferenced to county (centroid field = 'county')

#. provided_county_name

   * gbif/dwc county

#. provided_fips

   * gbif/dwc higherGeographyID

#. provided_state_name

   * gbif/dwc stateProvince

#. thumb_url

   * ignore

#. associated_media

   * not present in gbif occurrence.txt (2021, get from verbatim.txt)

#. associated_references

   * gbif/dwc associatedReferences

#. general_comments

   * gbif/dwc eventRemarks

#. id

   * gbif/dwc gbifID

#. provider_id

   * Calc: 'legacyid' from provider lookup table (LUT).
     LUT combines provider table from BISON Denver and
     GBIF organization API + publishing organization key (from dataset metadata)
     If legacyid does not exist for this provider, use the GBIF organization UUID

#. resource_id

   * Calc: 'legacyid' from resource lookup table (LUT).
     LUT combines resource table from BISON Denver and
     GBIF dataset API + dataset key
     If legacyid does not exist for this resource, use the GBIF dataset UUID

#. provided_common_name

   * gbif/dwc vernacularName

#. kingdom

   * Calc: gbif/dwc kingdom if in ['Animalia', 'Plantae', 'Bacteria', 'Fungi',
     'Protozoa', 'Chromista', 'Archaea', 'Virus'].
     If itis_tsn resolves, replace from ITIS lookup.

#. geodetic_datum

   * not present in GBIF occurrence.txt (2021, use from verbatim.txt)

#. coordinate_precision

   * gbif/dwc coordinatePrecision

#. coordinate_uncertainty

   * gbif/dwc coordinateUncertaintyInMeters

#. verbatim_locality

   * Calc: gbif/dwc 1) verbatimLocality 2) locality 3) habitat

#. mrgid

   * Calc: Point-in-polygon marine (use gridded EEZ), polygon + coordinates

#. calculated_waterbody

   * Calc: Point-in-polygon marine (use gridded EEZ), polygon + coordinates

#. establishment_means

   * Calc: after ITIS lookup, lookup from establishmentMeans table with
     itis_tsn (1st) or with clean_provided_scientific_name

#. iso_country_code

   * gbif/dwc countryCode

#. license

   * gbif/dc license



Misc Notes:
-------------

* Use ‘$’ delimiter in CSV output


Lookup Tables to assemble for Denver
--------------------------------------
#. Re-read all records, both BISON-provider and GBIF, to assemble list or
   lookup tables for Solr:
   * vernacular names(2 columns):
     * itis_common_name
     * itis_tsn
   * scientific_names(1 column):
     * clean_provided_scientific_name
   * provider (merge old BISON provider table with new BISON organization metadata):
     * name
     * provider_url
     * description
     * website_url
     * created
     * modified
     * deleted
     * display_name
     * BISONProviderID
     * OriginalProviderID
     * organization_id
   * resource (merge old BISON resource table with new GBIF dataset metadata):
     * BISONProviderID
     * name
     * display_name
     * description
     * rights
     * citation
     * logo_url
     * created
     * modified
     * deleted
     * website_url
     * override_citation
     * provider_id
     * OriginalResourceID
     * BISONResourceID
     * dataset_id
     * owningorganization_id
     * provider_url
     * provider_name

 * scientific_names: 1 column, bison_name filled with clean_provided_scientific_name
 * merge old BISON resoure table with new GBIF dataset metadata,
   add count for each resource
 * merge old BISON provider table with new BISON organization metadata,
   add count for each provider
