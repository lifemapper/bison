===============
LMBison Preparation
===============

-----------------------
Hardware requirements
-----------------------

Data processing for BISON annotation, summary, and statistics requires a powerful
machine with a large amount of available storage.  The most processing intensive
step, annotate, intersects each record with 4 geospatial data files.  This
implementation can run this process in parallel, and uses the number of CPUs on the
machine minus 2.  In Aug 2023, using 18 (of 20) cores, on 904 million
records, the process took 5 days.

These processes are all written in Python, and the implementation has been tested
on a machine running Ubuntu Linux.  Scripts will need minimal modification to run
on Windows or OSX successfully.

-----------------------
Download this Repository
-----------------------

The `LmBISON repository <https://github.com/lifemapper/bison>`_  can be installed by
downloading from Github.  This code repository contains scripts, Docker composition
files, configuration files, and test data for creating the outputs.

Type `git` at the command prompt to see if you have git installed.  If you do not,
download and install git from https://git-scm.com/downloads .

Download the LmBISON repository, containing test data and configurations, by typing at
the command line:

.. code-block::

   git clone https://github.com/lifemapper/bison

When the clone is complete, move to the top directory of the repository, `bison`.
All hands-on commands will be executed in a command prompt window from this
directory location.  In Linux or OSX, open a Terminal
window.

