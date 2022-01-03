#http://www.pythonforbeginners.com/python-on-the-web/how-to-use-urllib2-in-python/

import urllib2
import json
import codecs

out = codecs.open('outProvider.txt', 'w','utf-8')
infile = codecs.open('inProvider.txt', 'r','utf-8')


for line in infile:
    print "http://api.gbif.org/v0.9/dataset?identifier="+line.strip()+"&identifier_type=GBIF_PORTAL"
    responseProvider = urllib2.urlopen('http://api.gbif.org/v0.9/organization?identifier='+line.strip()+'&identifier_type=GBIF_PORTAL')

    # Getting the code
    print "This gets the code: ", responseProvider.code

    data = json.load(responseProvider)
    print "Length:", len(data)
    #print "Length:", data
    provider=data['results'][0]
    if provider.has_key("description"):
        descVal= provider['description']
    else:
        descVal=""
    if provider.has_key("homepage"):
        homepageVal= provider['homepage']
        if type(homepageVal) is list and len(homepageVal) > 0:
            homepageVal= provider['homepage'][0]
        elif type(homepageVal) is str:
            homepageVal= provider['homepage']
        else:
            homepageVal = ""
    else:
        homepageVal=""

    print line.strip()+','+provider['key']+','+provider['title']+','+descVal+','+provider['created']+','+provider['modified']+','+ homepageVal

    out.write(line.strip()+'|'+provider['key']+'|'+provider['title']+'|'+descVal+'|'+provider['created']+'|'+provider['modified']+'|'+ homepageVal+'\n')

    #l = json.dumps(data)

out.close()
infile.close()


#Name vs Display name
