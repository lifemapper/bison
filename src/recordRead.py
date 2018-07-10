import os
import xml
import json
import csv
import xml.etree.ElementTree as ET

ns = {'tdwg': 'http://rs.tdwg.org/dwc/text/'}
startCh = '/'
# nslen = len(ns)
pth = '/tank/data/input/bison/territories'
interpBase = 'occurrence.txt'
verbatimBase = 'verbatim.txt'
fieldmetaBase = 'meta.xml'
DELIMITER = '\t'

interpFname = os.path.join(pth, interpBase)
verbatimFname = os.path.join(pth, verbatimBase)
fieldmetaFname = '/tank/data/input/bison/us/meta.xml'

# .............................................................................
def getFileMeta():
   fields = {interpBase: {},
             verbatimBase: {} }
   tree = ET.parse(fieldmetaFname)
   root = tree.getroot()
   for child in root:
      fls = child.find('tdwg:files', ns)
      dloc = fls.find('tdwg:location', ns)
      currmeta = dloc.text
      if currmeta in fields.keys(): 
         flds = child.findall('tdwg:field')
         for fld in flds:
            idx = fld.get('index')
            temp = fld.get('term')
            term = temp[temp.rfind(startCh)+1:]
            fields[currmeta][term] = idx
   return fields
      
# .............................................................................
      
with open(interpFname, 'r') as interpF:
   occreader = csv.reader(interpF, delimiter=DELIMITER)
   for i, line in enumerate(interpF):
      print line
      if i == 4:
         break
      