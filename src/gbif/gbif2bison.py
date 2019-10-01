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
import datetime
import os
import time
import xml.etree.ElementTree as ET

from gbif.constants import (IN_DELIMITER, OUT_DELIMITER, PROHIBITED_VALS, 
                            VERBATIM, INTERPRETED, TERM_CONVERT, 
                            ORDERED_OUT_FIELDS, NAMESPACE, SAVE_FIELDS,
                            CLIP_CHAR, META_FNAME)
# todo: use unicodecsv library
from gbif.tools import getCSVReader, getCSVWriter, getLogger

        
# .............................................................................
class GBIFReader(object):
    """
    @summary: GBIF Record containing CSV record of 
                 * original provider data from verbatim.txt
                 * GBIF-interpreted data from occurrence.txt
    @note: To chunk the file into more easily managed small files (i.e. fewer 
             GBIF API queries), split using sed command output to file like: 
                sed -e '1,5000d;10000q' occurrence.txt > occurrence_lines_5000-10000.csv
             where 1-500 are lines to delete, and 1500 is the line on which to stop.
    """
    # ...............................................
    def __init__(self, interpretedFname, metaFname, outFname):
        """
        @summary: Constructor
        @param interpretedFname: Full filename containing records from the GBIF 
                 interpreted occurrence table
        @param metaFname: Full filename containing metadata for all data files in the 
                 Darwin Core GBIF Occurrence download:
                     https://www.gbif.org/occurrence/search
        @param outFname: Full filename for the output BISON CSV file
        """
#         self.gbifRes = GBIFCodes()
        self._files = []
        
        # Interpreted GBIF occurrence file
        self.interpFname = interpretedFname
        self._if = None
        self._files.append(self._if)
        self._iCsvrdr = None
        # GBIF metadata file for occurrence files
        self._metaFname = metaFname
        self.fldMeta = None
        
        # Output BISON occurrence file
        self.outFname = outFname
        self._outf = None
        self._files.append(self._outf)
        self._outWriter = None
        pth, outbasename = os.path.split(outFname)
        outbase, _ = os.path.splitext(outbasename)
        
        # Input for Canonical name lookup
        self.name4LookupFname = os.path.join(pth, 'nameUUIDForLookup.csv')
        self._name4lookupf = None
        self._files.append(self._name4lookupf)
        self._name4lookupWriter = None
        self._name4lookup = {}

        logname, _ = os.path.splitext(os.path.basename(__file__))
        logfname = os.path.join(pth, outbase + '.log')
        if os.path.exists(logfname):
            ts = int(time.time())
            logfname = os.path.join(pth, outbase + '.log.{}'.format(ts))
        self._log = getLogger(logname, logfname)
        
    # ...............................................
    def _timeStamp(self, msg):
        ts = time.time()
        st = datetime.datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')
        self._log.info('{}:    {}'.format(st, msg))

    # ...............................................
    def _cleanVal(self, val):
        val = val.strip()
        # TODO: additional conversion of unicode?
        if val.lower() in PROHIBITED_VALS:
            val = ''
        return val

    # ...............................................
    def _getValFromCorrectLine(self, fldname, meta, vline, iline):
        """
        @summary: IFF gathering values from separate lines (matched on gbifID)
                  use metadata to pull the value from the indicated line.
        @param fldname: field name 
        @param meta: tuple including datatype and INTERPRETED or VERBATIM 
               identifier for file to pull value from
        @param vline: A CSV record of original provider DarwinCore occurrence data 
        @param iline: A CSV record of GBIF-interpreted DarwinCore occurrence data 
        """
        # get column Index in correct file
        if meta['version'] == VERBATIM:
            try:
                val = vline[self.fldMeta[fldname][VERBATIM]]
            except KeyError:
                print('{} not in VERBATIM data, using INTERPRETED'.format(fldname))
                val = iline[self.fldMeta[fldname][INTERPRETED]]
        else:
            try:
                val = iline[self.fldMeta[fldname][INTERPRETED]]
            except KeyError:
                print('{} not in INTERPRETED data, using VERBATIM'.format(fldname))
                try:
                    val = vline[self.fldMeta[fldname][VERBATIM]]
                except Exception:
                    print('{} not in either file'.format(fldname))
        return val

    # ...............................................
    def _updatePoint(self, rec):
        """
        @summary: Update the decimal longitude and latitude, replacing 0,0 with 
                     None, None and ensuring that US points have a negative longitude.
        @param rec: dictionary of all fieldnames and values for this record
        @note: function modifies original dict
        @note: record must have lat/lon or countryCode but GBIF query is on countryCode so
               that will never be blank.
        """
        try:
            rec['decimalLongitude']
        except:
            rec['decimalLongitude'] = None

        try:
            rec['decimalLatitude']
        except:
            rec['decimalLatitude'] = None

        try:
            cntry = rec['countryCode']
        except:
            rec['countryCode'] = None
            
        # Change 0,0 to None
        if rec['decimalLongitude'] == 0 and rec['decimalLatitude'] == 0:
            rec['decimalLongitude'] = None
            rec['decimalLatitude'] = None
        # Make sure US longitude is negative
        elif (cntry == 'US' 
                and rec['decimalLongitude'] is not None 
                and rec['decimalLongitude'] > 0):
            rec['decimalLongitude'] = rec['decimalLongitude'] * -1 


    # ...............................................
    def _updateDates(self, rec):
        """
        @summary: Make sure that eventDate is parsable into integers and update 
                     missing year value by extracting from the eventDate.
        @param rec: dictionary of all fieldnames and values for this record
        @note: BISON eventDate should be ISO 8601, ex: 2018-08-01
                 GBIF combines with time (and here UTC time zone), ex: 2018-08-01T14:19:56+00:00
        """
        fillyr = False
        # Test year field
        try:
            rec['year'] = int(rec['year'])
        except:
            fillyr = True
            
        # Test eventDate field
        tmp = rec['eventDate']
        dateonly = tmp.split('T')[0]
        if dateonly != '':
            parts = dateonly.split('-')
            try:
                for i in range(len(parts)):
                    int(parts[i])
            except:
                pass
            else:
                rec['eventDate'] = dateonly
                if fillyr:
                    rec['year'] = parts[0]

    # ...............................................
    def _saveNameLookupData(self, rec):
        """
        @todo: Stop saving taxonKey - not useful if scientificName is not parsable 
        @summary: Save scientificName and taxonID for parse or API query 
                     respectively. 
        @param rec: dictionary of all fieldnames and values for this record
        @note: The name parser fails on unicode namestrings
        @note: Save to dictionary key=sciname, val=taxonid to avoid writing 
                 duplicates.  Append to file to save for later script runs, with 
                 lines like:
                      'scientificName, taxonKey [, taxonKey, taxonKey ...]'
        """
        sciname = taxkey = None
        try:
            sciname = rec['scientificName']
        except:
            sciname = ''
            
        try:
            taxkey = rec['taxonKey']
            try:
                int(taxkey)
            except:
                self._log.warn('gbifID {}: non-integer taxonID {}'.format(
                                    rec['gbifID'], taxkey))
                taxkey = ''
        except:
            self._log.warn('gbifID {}: missing taxonID {}'.format(rec['gbifID']))
            taxkey = ''
                
        saveme = True
        if taxkey != '':        
            try:
                keylist = self._name4lookup[sciname]
                if taxkey in keylist:
                    saveme = False
                else:
                    self._name4lookup[sciname].append(taxkey)
                    self._log.warn('gbifID {}: Sciname {} has multiple taxonIDs {}'.format(
                        rec['gbifID'], sciname, self._name4lookup[sciname]))
    
            except KeyError:
                self._name4lookup[sciname] = [taxkey]

            if saveme:
                row = [k for k in self._name4lookup[sciname]]
                row.insert(0, sciname)
                self._name4lookupWriter.writerow(row)
#         if (sciname == '' and taxkey == ''):
#             rec = None
#         else:
#             self._name4lookupWriter.writerow([rec['gbifID'], sciname, taxkey])


    # ...............................................
    def _updateFieldOrSignalRemove(self, fldname, val):
        """
        @summary: Update fields with any BISON-requested changed, or signal 
                     to remove the record by returning None for fldname and val.
        @param gbifID: GBIF identifier for this record, used just for logging
        @param fldname: Fieldname in current record
        @param val: Value for this field in current record
        """
        # Replace N/A
        if val.lower() in PROHIBITED_VALS:
            val = ''
            
        # simplify basisOfRecord terms
        elif fldname == 'basisOfRecord':
            if val in TERM_CONVERT.keys():
                val = TERM_CONVERT[val]
                
        # Convert year to integer
        elif fldname == 'year':
            try:
                val = int(val)
            except:
                val = '' 

        # remove records with occurrenceStatus  = absence
        elif fldname == 'occurrenceStatus' and val.lower() == 'absent':
            fldname = val = None
            
        # gather geo fields for check/convert
        elif fldname in ('decimalLongitude', 'decimalLatitude'):
            try:
                val = float(val)
            except Exception:
                val = None
            
        return fldname, val

    # ...............................................
    def _openForReadWrite(self, fname, header=None):
        '''
        @summary: Read and populate lookup table if file exists, open and return
                     file and csvwriter for writing or appending. If lookup file 
                     is new, write header if provided.
        '''
        lookupDict = {}
        doAppend = False
        
        if os.path.exists(fname):
            doAppend = True            
            try:
                csvRdr, infile = getCSVReader(fname, IN_DELIMITER)
                # get header
                line, recno = self.getLine(csvRdr, 0)
                # read lookup vals into dictionary
                while (line is not None):
                    line, recno = self.getLine(csvRdr, recno)
                    if line and len(line) > 0:
                        try:
                            # First item is dict key, rest are vals
                            lookupDict[line[0]] = line[1:]
                        except Exception:
                            self._log.warn('Failed to read line {} from {}'
                                                .format(recno, fname))
                self._log.info('Read lookup file {}'.format(fname))
            finally:
                infile.close()
        
        outWriter, outfile = getCSVWriter(fname, OUT_DELIMITER, doAppend=doAppend)
        self._log.info('Re-opened lookup file {} for appending'.format(fname))

        if not doAppend and header is not None:
            outWriter.writerow(header)
        
        return lookupDict, outWriter, outfile 
    
    
    # ...............................................
    def openInputOutput(self):
        '''
        @summary: Read GBIF metadata, open GBIF interpreted data for reading, 
                     output file for writing
        '''
        self.fldMeta = self.getFieldMeta()
        
        (self._iCsvrdr, 
         self._if) = getCSVReader(self.interpFname, IN_DELIMITER)
         
        (self._outWriter, 
         self._outf) = getCSVWriter(self.outFname, OUT_DELIMITER, doAppend=False)
        # Write the header row 
        self._outWriter.writerow(ORDERED_OUT_FIELDS)
        self._log.info('Opened input/output files')
         

    # ...............................................
    def openReadLookups(self):
        '''
        @summary: Read lookup files, then re-open for appending to 
                     save canonicalNames and organization UUIDs into files to avoid 
                     querying repeatedly.
        '''
        self._nameLookup, self._outCanWriter, self._outcf = \
                self._openForReadWrite(self.outCanonicalFname, numKeys=2, numVals=1,
                                header=['scientificName', 'taxonKey', 'canonicalName'])
                
        self._orgLookup, self._outOrgWriter, self._outof = \
                self._openForReadWrite(self.outOrgFname, numKeys=1, numVals=2,
                                header=['orgUUID', 'title', 'homepage'])
         
        self._datasetLookup, self._outDSWriter, self._outdf = \
                self._openForReadWrite(self.outDSFname, numKeys=1, numVals=3,
                                header=['datasetUUID', 'title', 'homepage', 'orgUUID'])


    # ...............................................
    def isOpen(self):
        """
        @summary: Return true if any files are open.
        """
        for f in self._files:
            if not f is None and not f.closed:
                return True
        return False

    # ...............................................
    def close(self):
        '''
        @summary: Close input datafiles and output file
        '''
        for f in self._files:
            try:
                f.close()
            except Exception:
                pass

    # ...............................................
    def getLine(self, csvreader, recno):
        '''
        @summary: Return a line while keeping track of the line number and errors
        '''
        success = False
        line = None
        while not success and csvreader is not None:
            try:
                line = csvreader.next()
                if line:
                    recno += 1
                success = True
            except OverflowError as e:
                recno += 1
                self._log.info( 'Overflow on record {}, line {} ({})'
                                     .format(recno, csvreader.line, str(e)))
            except StopIteration:
                self._log.info('EOF after record {}, line {}'
                                    .format(recno, csvreader.line_num))
                success = True
            except Exception as e:
                recno += 1
                self._log.info('Bad record on record {}, line {} ({})'
                                    .format(recno, csvreader.line, e))

        return line, recno
    
    # ...............................................
    def getFieldMeta(self):
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
        tree = ET.parse(self._metaFname)
        root = tree.getroot()
        # Child will reference INTERPRETED or VERBATIM file
        for child in root:
            fileElt = child.find('tdwg:files', NAMESPACE)
            fnameElt= fileElt .find('tdwg:location', NAMESPACE)
            currmeta = fnameElt.text
            sortedname = 'tidy_' + currmeta.replace('txt', 'csv')
            if sortedname.find(INTERPRETED) >= 0:
                flds = child.findall(tdwg+'field')
                for fld in flds:
                    idx = int(fld.get('index'))
                    temp = fld.get('term')
                    term = temp[temp.rfind(CLIP_CHAR)+1:]
                    if term in SAVE_FIELDS.keys():
                        dtype = SAVE_FIELDS[term][0]
                    else:
                        dtype = str
                    if not fields.has_key(term):
                        fields[term] = (idx, dtype)
        return fields
 
    # ...............................................
    def _updateFilterRec(self, rec):
        """
        @summary: Update record with all BISON-requested changes, or remove 
                     the record by setting it to None.
        @param rec: dictionary of all fieldnames and values for this record
<<<<<<< HEAD
=======
        @note: function modifies original dict
>>>>>>> f6a200938e32a91bc75701f5d60f17be57437773
        """
        gbifID = rec['gbifID']

        # Ignore absence record 
        if rec and rec['occurrenceStatus'].lower() == 'absent':
            self._log.info('gbifID {} with absence data discarded'.format(gbifID)) 
            rec = None
        
        if rec:
            # Format eventDate and fill missing year
            self._udpateDates(rec)
            
            # Save scientificName and TaxonID for later lookup and replace
            self._saveNameLookupData(rec)
            
            # Modify lat/lon vals if necessary
            self._updatePoint(rec)

    # ...............................................
    def createBisonLine(self, iline):
        """
        @summary: Create a list of values, ordered by BISON-requested fields in 
                     ORDERED_OUT_FIELDS, with individual values and/or entire record
                     modified according to BISON needs.
        @param iline: A CSV record of GBIF-interpreted DarwinCore occurrence data
        @return: list of ordered fields containing BISON-interpreted values for 
                    a single GBIF occurrence record. 
        """
        row = []
        gbifID = iline[0]
        rec = {'gbifID': gbifID}
        for fldname, (idx, _) in self.fldMeta.iteritems():
            try:
                tmpval = iline[idx]
            except Exception:
                self._log.warning('Failed to get column {}/{} for gbifID {}'
                                        .format(idx, fldname, gbifID))
                return row
            else:
                val = self._cleanVal(tmpval)
    
                fldname, val = self._updateFieldOrSignalRemove(fldname, val)
                if fldname is None:
                    return row
                else:
                    rec[fldname] = val
                
        # Update values and/or filter record out
        self._updateFilterRec(rec)
        
        if rec:
            # create the ordered row
            for fld in ORDERED_OUT_FIELDS:
                try:
                    row.append(rec[fld])
                except KeyError:
                    print ('Missing field {} in record with gbifID {}'
                             .format(fld, gbifID))
                    row.append('')

        return row

    # ...............................................
    def extractData(self):
        """
        @summary: Create a CSV file containing GBIF occurrence records extracted 
                     from the interpreted occurrence file provided 
                     from an Occurrence Download, in Darwin Core format.  Values may
                     be modified and records may be discarded according to 
                     BISON requests.
        @return: A CSV file of BISON-modified records from a GBIF download. 
        """
        if self.isOpen():
            self.close()
        if os.path.exists(self.outFname):
            raise Exception('Bison output file {} exists!'.format(self.outFname))
        try:
            self._name4lookup, self._name4lookupWriter, self._name4lookupf = \
                    self._openForReadWrite(self.name4LookupFname,
                                           header=['scientificName', 'taxonKey'])
            self.openInputOutput()
            # Pull the header row 
            line, recno = self.getLine(self._iCsvrdr, 0)
            while (line is not None):
                # Get interpreted record
                line, recno = self.getLine(self._iCsvrdr, recno)
                if line is None:
                    break
                # Create new record
                byline = self.createBisonLine(line)
                if byline:
                    self._outWriter.writerow(byline)
        except Exception as e:
            self._log.error('Failed on line {}, exception {}'.format(recno, e))
        finally:
            self.close()

# ...............................................
if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(
                description=("""Parse a GBIF occurrence dataset downloaded
                                     from the GBIF occurrence web service in
                                     Darwin Core format into BISON format.  
                                 """))
    parser.add_argument('gbif_file', type=str, 
                              help='The full pathname of the input GBIF occurrence file')
    parser.add_argument('bison_file', type=str, 
                              help=""""
                              The basefilename of the output BISON occurrence data.
                              Path will be the same as input data.
                              """)
    parser.add_argument('--names_only', type=bool, default=False,
                help=('Re-read a bison output file to retrieve scientificName and taxonID.'))
    args = parser.parse_args()
    rereadNames = args.names_only
    gbifFname = args.gbif_file
    bisonBaseFname = args.bison_file
    
    if os.path.exists(gbifFname):
        pth, basefname = os.path.split(gbifFname)
        metaFname = os.path.join(pth, META_FNAME)
        bisonFname = os.path.join(pth, bisonBaseFname)
        
        gr = GBIFReader(gbifFname, metaFname, bisonFname)
        print('Calling program with input/output {}'.format(gbifFname, bisonFname))
        gr.extractData()
    else:
        print('Filename {} does not exist'.format(gbifFname))
    
    
"""
# python /state/partition1/workspace/bison/src/gbif/gbif2bison.py 7500000 8000000
import os
import xml.etree.ElementTree as ET

from src.gbif.gbif2bison import *
from src.gbif.constants import *
from src.gbif.gbifapi import GBIFCodes
from src.gbif.tools import *

from src.gbif.gbif2bison import *


"""
