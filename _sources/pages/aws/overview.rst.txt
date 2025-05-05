Overview
#######################

The BISON AWS application is intended to allow USGS personnel with little
technical experience, and no local resources, to annotate and analyze the geographic
distribution of introduced and invasive species as indicated by specimen data in the
United States on a regular schedule.

The processes include annotating **GBIF occurrence data** from the
US with designations from the **US Registry of Introduced and Invasive Species**
(RIIS), summarizing the data by different regions, aggregating data by a
geospatial grid of counties, and finally, computing biodiversity statistics.

All processes have been implemented using Amazon Web Services (AWS) tools, removing
the need for powerful local compute resources.  These processes have been automated
to simplify execution on a regular schedule.

GBIF updates a subset of fields for all specimen records on the Registry of Open
Data (ODR) on AWS on the first day of every month.  BISON processing is therefore
scheduled to begin on the second day of every month.

Regions for Summary and Analysis
*********************************

Regions include **US Census state and county boundaries** and **American Indian,**
**Alaskan Native, and Native Hawaiian** (AIANNH).  States are required
in order to identify whether the occurrence of a particular species falls within the
a RIIS region (Alaska, Hawaii, or Lower 48), where it is identified as "Introduced"
or "Invasive".  Region summaries include both state and county boundaries.

Regions also include
regions and possibly **US Federal Protected Areas** (US‐PAD). US-PAD has proved
problematic.  For the step in which points are intersected these data, local processing
time was unreasonable, and processing in AWS Redshift failed entirely.

All regions (state, county, AIANNH, PAD?) will be summarized by count and proportion
for species, occurrences, and RIIS status.