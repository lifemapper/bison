#! /usr/bin/env python
#http://www.pythonforbeginners.com/python-on-the-web/how-to-use-urllib2-in-python/

import urllib2
import json
import codecs

out = codecs.open('outProviderAll.txt', 'w','utf-8')
#jsonout = codecs.open('outProviderAll.json', 'w','utf-8')
infile = codecs.open('inProvider.txt', 'w','utf-8')
newId=1000

print "http://api.gbif.org/v1/organization?limit=9999"
responseProvider = urllib2.urlopen('http://api.gbif.org/v1/organization?limit=9999')

# Getting the code
print "This gets the code: ", responseProvider.code
print "legacyid|key|title|description|created|modified|homepage"
out.write("legacyid|key|title|description|created|modified|homepage\n")
data = json.load(responseProvider)
print "Length:", len(data)
#print "Length:", data
allArray=data['results']
for provider in allArray:
    print provider['key']
    #provider=data['results'][0]
    if provider.has_key("description"):
        descVal = provider['description'].replace("\n", "\\n").replace("\r", "")
    else:
        descVal = ""
    if provider.has_key("homepage"):
        homepageVal= provider['homepage']
        if type(homepageVal) is list and len(homepageVal) > 0:
            homepageVal= provider['homepage'][0].replace("\n", "\\n").replace("\r", "")
        elif type(homepageVal) is str:
            homepageVal= provider['homepage'].replace("\n", "\\n").replace("\r", "")
        else:
            homepageVal = ""
    else:
        homepageVal=""
    if provider.has_key("identifiers") and len(provider['identifiers'])>0 and provider['identifiers'][0]['type']=='GBIF_PORTAL':
        legacyid=provider['identifiers'][0]['identifier']
        #print "has GBIF_PORTAL: " + legacyid
        infile.write(legacyid + '\n')
    else:
        #legacyid=str(newId)
        legacyid="9999"
        newId+=1
        #print "no GBIF_PORTAL: " + legacyid
    #print "begin data"
    #print legacyid
    #print provider['key']
    #print provider['title']
    #print provider['created']
    #print provider['modified']
    #print homepageVal
    #print "end data"
    #print legacyid+','+provider['key']+','+provider['title']+','+descVal+','+provider['created']+','+provider['modified']+','+ homepageVal

    out.write(legacyid.replace("\n", "\\n")+'|'+provider['key'].replace("\n", "\\n")+'|'+provider['title'].replace("\n", "\\n")+'|'+descVal.replace("\n", "\\n")+'|'+provider['created'].replace("\n", "\\n")+'|'+provider['modified'].replace("\n", "\\n")+'|'+ homepageVal.replace("\n", "\\n")+'\n')

    #l = json.dumps(data, indent=4, sort_keys=True)
    #jsonout.write( l + "\n")

out.close()
infile.close()

#Name vs Display name
