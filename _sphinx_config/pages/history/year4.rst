==================
Year 4, 2022-2023
==================

.. toctree::
    :glob:
    :maxdepth: 1

    pages/history/yr4_install
    pages/history/yr4_inputs
    pages/history/yr4_process

Objectives
----------------

2023, Year 4 SOW specifies building the project as a Docker application, so that USGS
personnel can run the analyses on their desktop machines, whether using a Windows, Mac,
or Linux operating system.

The initial processes include annotating **GBIF occurrence data** from the
US, with designations from the **US Registry of Introduced and Invasive Species**
(RIIS), then summarizing the data by different regions, then aggregating data by a
geospatial grid of counties, and computing biodiversity statistics.

Regions include **US Census state and county boundaries**.  States are required
in order to identify whether the occurrence of a particular species falls within the
a RIIS region (Alaska, Hawaii, or Lower 48), where it is identified as "Introduced"
or "Invasive".  Region summaries include both state and county boundaries.

Regions also include **American Indian, Alaskan Native, and Native Hawaiian** (AIANNH)
regions and **US Federal Protected Areas** (US‐PAD).

All regions (state, county, AIANNH, PAD) will be summarized by count and proportion
for species, occurrences, and RIIS status.
