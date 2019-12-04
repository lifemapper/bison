"""
@license: gpl2
@copyright: Copyright (C) 2019, University of Kansas Center for Research

             Lifemapper Project, lifemapper [at] ku [dot] edu, 
             Biodiversity Institute,
             1345 Jayhawk Boulevard, Lawrence, Kansas, 66045, USA
    
             This program is free software; you can redistribute it and/or modify 
             it under the terms of the GNU General Public License as published by 
             the Free Software Foundation; either version 2 of the License, or (at 
             your option) any later version.
  
             This program is distributed in the hope that it will be useful, but 
             WITHOUT ANY WARRANTY; without even the implied warranty of 
             MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU 
             General Public License for more details.
  
             You should have received a copy of the GNU General Public License 
             along with this program; if not, write to the Free Software 
             Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA 
             02110-1301, USA.
"""
from enum import Enum, auto

ENCODING = 'utf-8'
BISON_DELIMITER = '$'

NEWLINE = '\n'

# Rough log of processing progress
LOGINTERVAL = 1000000
LOG_FORMAT = ' '.join(["%(asctime)s",
                       "%(funcName)s",
                       "line",
                       "%(lineno)d",
                       "%(levelname)-8s",
                       "%(message)s"])
LOG_DATE_FORMAT = '%d %b %Y %H:%M'
LOGFILE_MAX_BYTES = 52000000 
LOGFILE_BACKUP_COUNT = 5

ITIS_SOLR_URL = 'https://services.itis.gov/'
ITIS_NAME_KEY = 'nameWOInd'
ITIS_TSN_KEY = 'tsn'
ITIS_URL_ESCAPES = [ [" ", "\%20"] ]
ITIS_VERNACULAR_QUERY = 'https://www.itis.gov/ITISWebService/services/ITISService/getCommonNamesFromTSN?tsn='
ITIS_NAMESPACE = '{http://itis_service.itis.usgs.gov}'
ITIS_DATA_NAMESPACE = '{http://data.itis_service.itis.usgs.gov/xsd}'
W3_NAMESPACE =  '{http://www.w3.org/2001/XMLSchema-instance}'

LICENSE =  '(http://creativecommons.org/publicdomain/zero/1.0/legalcode) (DwC: license)'

class FillMethod(Enum):
    gbif_meta = auto()
    gbif_name = auto()
    itis_tsn = auto()
    georef = auto()
    est_means = auto()
    unknown = auto()

    @staticmethod
    def is_calc(mtype):
        if mtype in (FillMethod.gbif_meta,
                     FillMethod.gbif_name,
                     FillMethod.itis_tsn, 
                     FillMethod.georef, 
                     FillMethod.est_means, 
                     FillMethod.unknown):
            return True
        else:
            return False

    @staticmethod
    def in_pass1(mtype):
        if mtype in (FillMethod.gbif_name,
                     FillMethod.itis_tsn, 
                     FillMethod.georef, 
                     FillMethod.est_means, 
                     FillMethod.unknown):
            return False
        else:
            return True

    @classmethod
    def pass1(cls):
        return cls.gbif_meta
    
            
FillLater = (FillMethod.itis_tsn, FillMethod.georef, FillMethod.est_means, 
             FillMethod.unknown)
