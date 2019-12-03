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
import glob
import os
import xml.etree.ElementTree as ET

from common.constants import (BISON_DELIMITER, ENCODING)
from common.tools import (getCSVDictReader, getCSVWriter)
                            
from gbif.constants import (INTERPRETED, CLIP_CHAR, NAMESPACE, GBIF_ORG_FOREIGN_KEY)
from gbif.gbifapi import GbifAPI
        
# .............................................................................
class GBIFMetaReader(object):
    """
    @summary: GBIF Record containing CSV record of 
                 * original provider data from verbatim.txt
                 * GBIF-interpreted data from occurrence.txt
    @note: To chunk the file into more easily managed small files (i.e. fewer 
             GBIF API queries), split using sed command output to file like: 
                sed -e '1,5000d;10000q' occurrence.txt > occurrence_lines_5000-10000.csv
             where 1-5000 are lines to delete, and 10000 is the line on which to stop.
    """
    # ...............................................
    def __init__(self, log):
        """
        @summary: Constructor
        """
        self._log = log

    # ...............................................
    def get_field_meta(self, meta_fname):
        '''
        @todo: Remove interpreted / verbatim file designations, interpreted cannot 
                 be joined to verbatim file for additional info.
        @summary: Read metadata for interpreted data file, and
                     for fields we are interested in:
                     extract column index for each file, add datatype. 
                     Resulting metadata will look like:
                            fields = {term: (columnIndex, dtype), 
                                         ...
                                         }
        '''
        tdwg = '{http://rs.tdwg.org/dwc/text/}'
        fields = {}
        tree = ET.parse(meta_fname)
        root = tree.getroot()
        # Child will reference INTERPRETED or VERBATIM file
        for child in root:
            # does this node of metadata reference INTERPRETED or VERBATIM?
            fileElt = child.find('tdwg:files', NAMESPACE)
            fnameElt= fileElt .find('tdwg:location', NAMESPACE)
            meta4data = fnameElt.text

            if meta4data.startswith(INTERPRETED):
                flds = child.findall(tdwg+'field')
                for fld in flds:
                    # Get column num and short name
                    idx = int(fld.get('index'))
                    temp = fld.get('term')
                    term = temp[temp.rfind(CLIP_CHAR)+1:]
                    # Save all fields
                    if not term in fields:
                        fields[term] = idx
                    else:
                        self._log.info('Duplicate field {}, idxs {} and {}'
                                       .format(term, fields[term], idx))
        return fields

    # ...............................................
    def get_dataset_uuids(self, dataset_pth):
        """
        @summary: Get dataset UUIDs from downloaded dataset EML filenames.
        @param dataset_pth: absolute path to the dataset EML files
        """
        uuids = []
        dsfnames = glob.glob(os.path.join(dataset_pth, '*.xml'))
        if dsfnames is not None:
            start = len(dataset_pth)
            if not dataset_pth.endswith(os.pathsep):
                start += 1
            stop = len('.xml')
            for fn in dsfnames:
                uuids.append(fn[start:-stop])
        self._log.info('Read {} dataset UUIDs from filenames in {}'
                       .format(len(uuids), dataset_pth))
        return uuids
    
    # ...............................................
    def get_organization_uuids(self, dset_lut_fname):
        """
        @summary: Get organization UUIDs from dataset metadata pulled from GBIF
                  and written to the dset_lut_fname file
        @param dset_lut_fname: dataset lookup table filename with absolute path
        """
        org_uuids = set()
        try:
            rdr, inf = getCSVDictReader(dset_lut_fname, BISON_DELIMITER, ENCODING)
            for dset_data in rdr:
                orgUUID = dset_data[GBIF_ORG_FOREIGN_KEY]
                org_uuids.add(orgUUID) 
        except Exception as e:
            print('Failed read {} ({})'.format(dset_lut_fname, e))
        finally:
            inf.close()
        self._log.info('Read {} unique organiziation UUIDs from datasets in {}'
                       .format(len(org_uuids), dset_lut_fname))
        return org_uuids
        

    # ...............................................
    def write_resolved_taxkeys(self, lut_fname, name_fails, nametaxa):
        """
        @summary: Create lookup table for: 
                  BISON canonicalName from GBIF scientificName and/or taxonKey
        """
        csvwriter, f = getCSVWriter(lut_fname, BISON_DELIMITER, ENCODING, 
                                    fmode='a')
        count = 0
        tax_resolved = []
        gbifapi = GbifAPI()
        try:
            for badname in name_fails:
                taxonkeys = nametaxa[badname]
                for tk in taxonkeys:
                    canonical = gbifapi.find_canonical(taxkey=tk)
                    if canonical is not None:
                        count += 1
                        csvwriter.writerow([tk, canonical])
                        self._log.info('Appended {} taxonKey/clean_provided_scientific_name to {}'
                                       .format(count, lut_fname))
                        tax_resolved.append(badname)
                        break
        except Exception as e:
            pass
        finally:
            f.close()
        self._log.info('Wrote {} taxkey/canonical pairs ({} failed) to {}'
                       .format(len(tax_resolved), 
                               len(name_fails) - len(tax_resolved), lut_fname))                    
        for tres in tax_resolved:
            name_fails.remove(tres)        
        return name_fails
            
    # ...............................................
    def write_parsed_names(self, lut_fname, namelst):
        """
        @summary: Create lookup table for: 
                  BISON canonicalName from GBIF scientificName and/or taxonKey
        """
        tot = 1000 
        name_fails = []
        gbifapi = GbifAPI()
        while namelst:
            currnames = namelst[:tot]
            namelst = namelst[tot:]
            total, currfail = gbifapi.get_write_parsednames(currnames, lut_fname)
            name_fails.extend(currfail)
            self._log.info('Wrote {} sciname/canonical pairs ({} failed) to {}'
                           .format(total-len(currfail), len(currfail), lut_fname))
        return name_fails
            
    # ...............................................
    def write_name_lookup(self, logname=None):
        """
        @summary: Create lookup table for: 
                  BISON canonicalName from GBIF scientificName and/or taxonKey
        """
        if not os.path.exists(self.nametaxa_fname):
            raise Exception('Input file {} missing!'.format(self.nametaxa_fname))
        if os.path.exists(self.name_lut_fname):
            raise Exception('Output LUT file {} exists!'.format(self.name_lut_fname))
        self._rotate_logfile(logname=logname)
        
        # Read name/taxonIDs dictionary for name resolution
        nametaxa = self._read_name_taxa(self.nametaxa_fname)
        namelst = list(nametaxa.keys())
        gbifapi = GbifAPI()
        
        name_fails = self._write_parsed_names(namelst, gbifapi)        
        name_fails = self._write_resolved_taxkeys(name_fails, nametaxa, gbifapi)
        
        return name_fails
            
# ...............................................
if __name__ == '__main__':
    pass