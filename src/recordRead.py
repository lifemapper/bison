import os
import xml
import json
import csv
import xml.etree.ElementTree as ET


interpFname = 'occurrence.txt'
verbatimFname = 'verbatim.txt'
fieldmetaFname = 'meta.xml'

tree = ET.parse(fieldmetaFname)
root = tree.getroot()
for child in root:
   print child.tag, child.attrib

with open(interpFname, 'r') as interpF:
   for i, line in enumerate(interpF):
      print line
      if i == 4:
         break
      