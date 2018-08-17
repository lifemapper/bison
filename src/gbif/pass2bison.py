"""
@license: gpl2
@copyright: Copyright (C) 2018, University of Kansas Center for Research

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

from constants import *
from tools import getCSVReader, getCSVWriter, getLogger
      
# .............................................................................
class BisonReader(object):
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
   def __init__(self, infname, outfname):
      """
      @summary: Constructor
      @param infname: Full filename for the input CSV file
      @param outfname: Full filename for the output CSV file
      """
      self._files = []
      pth, basefname = os.path.split(infname) 
      base, ext = os.path.splitext(basefname) 
      
      
      self.infname = infname
      self._inf = None
      self._files.append(self._inf)
      self._inRdr = None
            
      self.outfname = outfname
      self._outf = None
      self._files.append(self._outf)
      self._outWtr = None
            
      # Input for Canonical name lookup
      self.name4LookupFname = os.path.join(pth, 'nameUUIDForLookup.csv')
      self._name4lookupf = None
      self._files.append(self._name4lookupf)
      self._name4lookupWriter = None
      self._name4lookup = {}

      # Output from parsing GBIF Name parser batch results
      # Input to 2nd pass, resolving scientificNames into canonicalNames
      self.parsedfname = os.path.join(pth, 'parsedScinames.csv')
      self._parsedf = None
      self._files.append(self._parsedf)

      logname, _ = os.path.splitext(os.path.basename(__file__))
      logfname = os.path.join(pth, base + '.log')
      if os.path.exists(logfname):
         import time
         ts = int(time.time())
         logfname = os.path.join(pth, base + '.log.{}'.format(ts))
      self._log = getLogger(logname, logfname)
      

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
            csvRdr, infile = getCSVReader(fname, DELIMITER)
            # get header
            line, recno = self.getLine(csvRdr, 0)
            # read lookup vals into dictionary
            while (line is not None):
               line, recno = self.getLine(csvRdr, recno)
               if line and len(line) > 0:
                  try:
                     # First item is dict key, rest are vals
                     lookupDict[line[0]] = line[1:]
                  except Exception, e:
                     self._log.warn('Failed to read line {} from {}'
                                    .format(recno, fname))
            self._log.info('Read lookup file {}'.format(fname))
         finally:
            infile.close()
      
      outWriter, outfile = getCSVWriter(fname, DELIMITER, doAppend=doAppend)
      self._log.info('Re-opened lookup file {} for appending'.format(fname))

      if not doAppend and header is not None:
         outWriter.writerow(header)
      
      return lookupDict, outWriter, outfile 
   
   
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
         except Exception, e:
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
         except OverflowError, e:
            recno += 1
            self._log.info( 'Overflow on record {}, line {} ({})'
                            .format(recno, csvreader.line, str(e)))
         except StopIteration:
            self._log.info('EOF after record {}, line {}'
                           .format(recno, csvreader.line_num))
            success = True
         except Exception, e:
            recno += 1
            self._log.info('Bad record on record {}, line {} ({})'
                           .format(recno, csvreader.line, e))

      return line, recno
   
   # ...............................................
   def rewriteBisonLine(self, line):
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
      #self._log.info('')
      #self._log.info('Record {}'.format(gbifID))
      for fldname, (idx, dtype) in self.fldMeta.iteritems():
         try:
            tmpval = iline[idx]
         except Exception, e:
            self._log.warning('Failed to get column {}/{} for gbifID {}'
                              .format(idx, fldname, gbifID))
            return row
         else:
            val = self._cleanVal(tmpval)
   
            fldname, val = self._updateFieldOrSignalRemove(gbifID, fldname, val)
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
            except KeyError, e:
               print ('Missing field {} in record with gbifID {}'
                      .format(fld, gbifID))
               row.append('')

      return row

   # ...............................................
   def _readScinameTaxkeyForLookup(self, line, idIdx, snIdx, tkIdx):
      """
      @summary: Save scientificName and taxonID for parse or API query 
                respectively. 
      @param rec: dictionary of all fieldnames and values for this record
      @note: The name parser fails on unicode namestrings
      @note: Save to dictionary key=sciname, val=taxonid to avoid writing 
             duplicates.  Append to file to save for later script runs, with 
             lines like:
                 'scientificName, taxonKey [, taxonKey, taxonKey ...]'
      """
      row = None
      try:
         sciname = line[snIdx]
      except:
         sciname = ''
          
      try:
         taxkey = line[tkIdx]
         try:
            int(taxkey)
         except:
            self._log.warn('gbifID {}: non-integer taxonID {}'.format(
                           line[idIdx], taxkey))
            taxkey = ''
      except:
         self._log.warn('gbifID {}: missing taxonID {}'.format(line[idIdx]))
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
                  line[idIdx], sciname, self._name4lookup[sciname]))
    
         except KeyError:
            self._name4lookup[sciname] = [taxkey]
 
         if saveme:
            row = [k for k in self._name4lookup[sciname]]
            row.insert(0, sciname)
            self._name4lookupWriter.writerow(row)
       
      return row
 
   # ...............................................
   def _getCanonicalName(self, line, idIdx, snIdx, tkIdx):
      """
      @summary: Save scientificName and taxonID for parse or API query 
                respectively. 
      @param rec: dictionary of all fieldnames and values for this record
      @note: The name parser fails on unicode namestrings
      @note: Save to dictionary key=sciname, val=taxonid to avoid writing 
             duplicates.  Append to file to save for later script runs, with 
             lines like:
                 'scientificName, taxonKey [, taxonKey, taxonKey ...]'
      """
      row = None
      try:
         sciname = line[snIdx]
      except:
         sciname = ''
          
      try:
         taxkey = line[tkIdx]
         try:
            int(taxkey)
         except:
            self._log.warn('gbifID {}: non-integer taxonID {}'.format(
                           line[idIdx], taxkey))
            taxkey = ''
      except:
         self._log.warn('gbifID {}: missing taxonID {}'.format(line[idIdx]))
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
                  line[idIdx], sciname, self._name4lookup[sciname]))
    
         except KeyError:
            self._name4lookup[sciname] = [taxkey]
 
         if saveme:
            row = [k for k in self._name4lookup[sciname]]
            row.insert(0, sciname)
            self._name4lookupWriter.writerow(row)
       
      return row
 
 
   # ...............................................
   def populateCanonicalNames(self):
      """
      @summary: Read a CSV file containing BISON formatted data to retrieve.
      @return: A CSV file of BISON-modified records from a GBIF download. 
      """
      if self.isOpen():
         self.close()
      if not os.path.exists(self.outFname):
         raise Exception('Bison input file {} does not exist!'.format(self.outFname))
      try:
         (self._iCsvrdr, self._if) = getCSVReader(self.outFname, DELIMITER)
 
         self._name4lookup, self._name4lookupWriter, self._name4lookupf = \
               self._openForReadWrite(self.name4LookupFname,
                                      header=['scientificName', 'taxonKey'])
         # Pull the BISON header row 
         line, recno = self.getLine(self._iCsvrdr, 0)
         idIdx = line.index('gbifID')
         snIdx = line.index('scientificName')
         tkIdx = line.index('taxonKey')
         while (line is not None):
            # Get interpreted record
            line, recno = self.getLine(self._iCsvrdr, recno)
            if line is None:
               break
            # Create new record
            row = self._readScinameTaxkeyForLookup(line, idIdx, snIdx, tkIdx)
            if row:
               self._name4lookupWriter.writerow(row)
      except Exception, e:
         self._log.error('Failed on line {}, exception {}'
                         .format(self._iCsvrdr.line_num, e))
      finally:
         self.close()
         
   # ...............................................
   def rereadNames(self, infile, outfile):
      raise Exception('rereadNames is not yet implemented')


# ...............................................
if __name__ == '__main__':
   import argparse
   parser = argparse.ArgumentParser(
            description=("""Parse elements of a BISON-formatted occurrence 
                            dataset for a second pass to:
                               * pull scientific names from a lookup table 
                                 created on initial gbif to bison parsing, 
                                 into a file suitable for input into the GBIF 
                                 name-parsing API
                               * pull taxonIDs from a lookup table 
                                 created on initial gbif to bison parsing,
                                 discarding those for which the scientific
                                 name was already parsed.  As the taxonIDs are
                                 encountered, resolve them with the GBIF API
                               * parse the results of the GBIF name-parsing API
                                 into a lookup table for replacement.  
                            from the GBIF occurrence web service in
                            Darwin Core format into BISON format.  
                         """))
   parser.add_argument('command', type=str, choices=['pull_name_id', 'pull_name',
                                                     'read_parsed', 'fill_parsed', 
                                                     'fill_resolved'],
                       help="""
                       The type of processing to perform.  Choices are:
                       * pull_name_id: Pull all scientificName/taxonId pairs from 
                                    input BISON CSV file into a output 
                                    name/UUID lookup file.
                       * pull_name: Pull the scientificName from input name/UUID 
                                    lookup file into output scientificName file for 
                                    sending to GBIF name parser web service.
                       * read_parsed: Read the input GBIF name parser web service
                                    results and write an output lookup file
                                    'parsedScinames.csv' with 
                                    scientificName / canonicalName.
                       * fill_parsed: Fill canonicalName from input BISON CSV file 
                                    and 'parsedScinames.csv' lookup file into a 
                                    output 2nd pass BISON CSV file. 
                       * fill_resolved: Fill canonicalName not yet filled, from 
                                    input 2nd pass BISON CSV file into a output 
                                    3rd pass BISON CSV file.
                       """)
   parser.add_argument('--infile', type=str, default='',
                       help='The full pathname of an input file for this command.')
   parser.add_argument('--outfile', type=str, default='',
                       help='The full pathname of an output file for this command.')

   args = parser.parse_args()
   infile = args.infile
   outfile = args.outfile
   cmd = args.command
   
   if os.path.exists(infile):
      pth, basefname = os.path.split(infile)
      if not os.path.exists(infile):
         raise Exception('Infile {} does not exist'.format(infile))
      if outfile is not '' and os.path.exists(infile):
         raise Exception('Outfile {} exists!'.format(outfile))
      
      br = BisonReader(infile, outfile)
      if cmd == 'pull_name':
         br.rereadNames(infile, outfile)
         print('Calling program with input {}, output {}'.format(infile, outfile))
   else:
      print('Filename {} does not exist'.format(infile))

   
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
   
   
   
         
