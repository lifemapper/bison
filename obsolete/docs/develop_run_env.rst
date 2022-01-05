--------------------------------------------------
Requirements for Develop or Run of BISON data load
--------------------------------------------------

Python
======

Interpreter
***********
    * Python 3.6
        module load opt-python
        python3.6 --version

Dependencies (running on CentOS 7.6)
**************************************

Python dependencies
====================
    * pip3.6 (needed for setuptools)
        python3.6 -m ensurepip --default-pip
        pip3 --version

        You are using pip version 9.0.1, however version 19.2.3 is available.
        You should consider upgrading via the 'pip install --upgrade pip' command.

        pip3 install --upgrade pip

    * setuptools (needed for requests)
        ARCHIVENAME=setuptools
        VERSION=41.2.0
        SRC_URL=https://files.pythonhosted.org/packages/d9/ca/7279974e489e8b65003fe618a1a741d6350227fa2bf48d16be76c7422423/$ARCHIVENAME-$VERSION.zip
        wget $SRC_URL

        unzip $ARCHIVENAME-$VERSION.zip
        cd $ARCHIVENAME-$VERSION
        module load opt-python
        $PYPATH setup.py install

    * requests 2.22.0

        ARCHIVENAME=requests
        VERSION=2.22.0
        SRC_URL=https://github.com/kennethreitz/$ARCHIVENAME/archive/v$VERSION.tar.gz
        OUTFILE=$ARCHIVENAME-$VERSION.tar.gz
        wget --output-document=$OUTFILE $SRC_URL

    * geos 0.2.2
        ARCHIVENAME=geos
        VERSION=0.2.2
        SRC_URL=https://files.pythonhosted.org/packages/11/9b/a190f02fb92f465a7640b9ee7da732d91610415a1102f6e9bb08125a3fef/$ARCHIVENAME-$VERSION.tar.gz
        tar -xzvf $ARCHIVENAME-$VERSION.tar.gz
        cd $ARCHIVENAME-$VERSION
        module load opt-python
        python3.6 setup.py install

    * gdal
        ARCHIVENAME=gdal
        VERSION=1.11.4
        tar xzfv gdal-1.11.4.tar.gz
        cd gdal-1.11.4/
        module load opt-python
        ./configure --with-python=/opt/python/bin/python3.6 --with-ogr --with-proj --with-geos --prefix=/usr

    * rtree 0.9.3
        ARCHIVENAME=Rtree
        VERSION=0.9.3
        SRC_URL=https://files.pythonhosted.org/packages/11/1d/42d6904a436076df813d1df632575529991005b33aa82f169f01750e39e4/$ARCHIVENAME-$VERSION.tar.gz
        wget $SRC_URL
        tar -xzvf $ARCHIVENAME-$VERSION.tar.gz
        cd $ARCHIVENAME-$VERSION
        module load opt-python
        python3.6 setup.py install



      * https://pypi.org/project/Rtree/

      * requires libspatialindex (install from yum below)


    * Pypmpler 0.7 (for memory logging)

        pip3 install pympler

Install dependencies for Rocks 7.0 (as root)::
        PYVER=python3.6
        PYPATH=/opt/python/bin/$PYVER

        tar xzvf $OUTFILE
        cd $ARCHIVENAME-$VERSION

        module load opt-python
        $PYPATH setup.py install

System dependencies
====================

libaec-1.0.4-1.el7.x86_64
libaec-devel-1.0.4-1.el7.x86_64
hdf5-1.8.12-11.el7.x86_64
hdf5-devel-1.8.12-11.el7.x86_64
proj-4.8.0-4.el7.x86_64.rpm

yum install --enablerepo epel spatialindex (installs 1.8.5)
yum install --enablerepo epel geos (installs 3.4.2-2.el7.x86_64)
yum install --enablerepo epel geos-devel
yum install --enablerepo epel geos-python

geos-python-3.4.2-2.el7.x86_64
geos-3.4.2-2.el7.x86_64
geos-devel-3.4.2-2.el7.x86_64

This may be unnecessary as we make from source below.  But dependencies that it
brings in may be needed
# yum install --enablerepo epel gdal
# yum install --enablerepo epel gdal-devel
Installing:
 gdal-devel                                                      x86_64                                                 1.11.4-3.el7                                                          epel                                                      130 k
Installing for dependencies:
 CharLS                                                          x86_64                                                 1.0-5.el7                                                             epel                                                       63 k
 SuperLU                                                         x86_64                                                 5.2.0-5.el7                                                           epel                                                      180 k
 armadillo                                                       x86_64                                                 8.600.1-2.el7                                                         epel                                                       29 k
 arpack                                                          x86_64                                                 3.1.3-2.el7                                                           epel                                                      101 k
 atlas                                                           x86_64                                                 3.10.1-12.el7                                                         Rocks-7.0                                                 4.5 M
 blas                                                            x86_64                                                 3.4.2-8.el7                                                           Rocks-7.0                                                 399 k
 cfitsio                                                         x86_64                                                 3.370-10.el7                                                          epel                                                      528 k
 freexl                                                          x86_64                                                 1.0.5-1.el7                                                           epel                                                       32 k
 gdal-libs                                                       x86_64                                                 1.11.4-3.el7                                                          epel                                                      4.4 M
 lapack                                                          x86_64                                                 3.4.2-8.el7                                                           Rocks-7.0                                                 5.4 M
 libdap                                                          x86_64                                                 3.13.1-2.el7                                                          epel                                                      423 k
 libgeotiff                                                      x86_64                                                 1.2.5-14.el7                                                          epel                                                      545 k
 libgta                                                          x86_64                                                 1.0.4-1.el7                                                           epel                                                       32 k
 netcdf                                                          x86_64                                                 4.3.3.1-5.el7                                                         epel                                                      693 k
 ogdi                                                            x86_64                                                 3.2.0-0.19.beta2.el7                                                  epel                                                      248 k
 openblas-openmp                                                 x86_64                                                 0.3.3-2.el7                                                           epel                                                      4.4 M
 openjpeg2                                                       x86_64                                                 2.3.1-1.el7                                                           epel                                                      153 k
 postgresql-libs                                                 x86_64                                                 9.2.24-1.el7_5                                                        Rocks-7.0                                                 234 k
 unixODBC                                                        x86_64                                                 2.3.1-11.el7                                                          Rocks-7.0                                                 413 k
 xerces-c                                                        x86_64                                                 3.1.1-9.el7                                                           Rocks-7.0                                                 878 k

# ldconfig /usr/lib64

Install gdal for python from source, otherwise yum forces it to 2.7.5 in /usr/bin/python
# tar xzfv gdal-1.11.4.tar.gz
# cd gdal-1.11.4/
# module load opt-python
# ./configure --with-python=/opt/python/bin/python3.6 --with-ogr --with-proj --with-geos --prefix=/usr

GDAL is now configured for x86_64-unknown-linux-gnu

  Installation directory:    /usr
  C compiler:                gcc -g -O2 -DHAVE_SSE_AT_COMPILE_TIME
  C++ compiler:              g++ -g -O2 -DHAVE_SSE_AT_COMPILE_TIME

  LIBTOOL support:           yes

  LIBZ support:              external
  LIBLZMA support:           no
  GRASS support:             no
  CFITSIO support:           no
  PCRaster support:          internal
  LIBPNG support:            external
  DDS support:               no
  GTA support:               no
  LIBTIFF support:           internal (BigTIFF=yes)
  LIBGEOTIFF support:        internal
  LIBJPEG support:           external
  12 bit JPEG:               no
  12 bit JPEG-in-TIFF:       no
  LIBGIF support:            internal
  OGDI support:              no
  HDF4 support:              no
  HDF5 support:              yes
  NetCDF support:            no
  Kakadu support:            no
  JasPer support:            no
  OpenJPEG support:          no
  ECW support:               no
  MrSID support:             no
  MrSID/MG4 Lidar support:   no
  MSG support:               no
  GRIB support:              yes
  EPSILON support:           no
  WebP support:              no
  cURL support (wms/wcs/...):yes
  PostgreSQL support:        no
  MySQL support:             no
  Ingres support:            no
  Xerces-C support:          no
  NAS support:               no
  Expat support:             yes
  libxml2 support:           yes
  Google libkml support:     no
  ODBC support:              no
  PGeo support:              no
  FGDB support:              no
  MDB support:               no
  PCIDSK support:            internal
  OCI support:               no
  GEORASTER support:         no
  SDE support:               no
  Rasdaman support:          no
  DODS support:              no
  SQLite support:            no
  PCRE support:              yes
  SpatiaLite support:        no
  DWGdirect support          no
  INFORMIX DataBlade support:no
  GEOS support:              yes
  Poppler support:           no
  Podofo support:            no
  OpenCL support:            no
  Armadillo support:         no
  FreeXL support:            no
  SOSI support:              no


  SWIG Bindings:             python

  Statically link PROJ.4:    no
  enable OGR building:       yes
  enable pthread support:    yes
  enable POSIX iconv support:yes
  hide internal symbols:     no


# make prefix=/usr ROOT=/ install
# cd swig/python/
# python3.6 setup.py build
# python3.6 setup.py install

[astewart@badenov git]$ python3.6
Python 3.6.2 (default, Dec  1 2017, 22:03:46)
[GCC 4.8.5 20150623 (Red Hat 4.8.5-16)] on linux
Type "help", "copyright", "credits" or "license" for more information.
>>> import gdal
Traceback (most recent call last):
  File "<stdin>", line 1, in <module>
  File "/opt/python/lib/python3.6/site-packages/GDAL-1.11.4-py3.6-linux-x86_64.egg/gdal.py", line 2, in <module>
    from osgeo.gdal import deprecation_warn
  File "/opt/python/lib/python3.6/site-packages/GDAL-1.11.4-py3.6-linux-x86_64.egg/osgeo/__init__.py", line 21, in <module>
    _gdal = swig_import_helper()
  File "/opt/python/lib/python3.6/site-packages/GDAL-1.11.4-py3.6-linux-x86_64.egg/osgeo/__init__.py", line 17, in swig_import_helper
    _mod = imp.load_module('_gdal', fp, pathname, description)
  File "/opt/python/lib/python3.6/imp.py", line 242, in load_module
    return load_dynamic(name, filename, file)
  File "/opt/python/lib/python3.6/imp.py", line 342, in load_dynamic
    return _load(spec)
ImportError: libgdal.so.1: cannot open shared object file: No such file or directory

[root@badenov ~]# ldconfig -p | grep libgdal
    libgdal.so.1 (libc6,x86-64) => /root/gdal-1.11.4/.libs/libgdal.so.1
    libgdal.so (libc6,x86-64) => /root/gdal-1.11.4/.libs/libgdal.so
ldconfig /root/gdal-1.11.4/.libs/


    * gdal with dependencies from epel repo:
       yum install  --enablerepo=epel gdal
