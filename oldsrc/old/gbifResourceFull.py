#http://www.pythonforbeginners.com/python-on-the-web/how-to-use-urllib2-in-python/

import urllib2
import json
import codecs

out = codecs.open('outResource.txt', 'w','utf-8')
infile = codecs.open('inr.txt', 'r','utf-8')

#responseResource =urllib2.urlopen('http://api.gbif.org/v0.9/dataset?identifier=14348&identifier_type=GBIF_PORTAL')
#responseresource= urllib2.urlopen(' http://api.gbif.org/v0.9/organization?identifier=49&identifier_type=GBIF_PORTAL')

for line in infile:
        print "http://api.gbif.org/v0.9/dataset?identifier=",line.strip(),"&identifier_type=GBIF_PORTAL"
	responseresource = urllib2.urlopen('http://api.gbif.org/v0.9/dataset?identifier='+line.strip()+'&identifier_type=GBIF_PORTAL')

	# Getting the code
	print "This gets the code: ", responseresource.code

	# Get all data
	#html = responseresource.read()
	#print "Get all data: ", html

	print "Resource ID:", line
	data = json.load(responseresource)
	resource=data['results'][0]
	if resource.has_key("rights"):
		rightsVal=resource['rights']
	else:
		rightsVal=""
	if resource.has_key("logoUrl"):
		logoVal=resource['logoUrl']
	else:
		logoVal=""
	if resource.has_key("homepage"):
		homepageVal=resource['homepage']
	else:
		homepageVal=""
	if resource.has_key("description"):
		descVal=resource['description']
	else:
		descVal=""
	print resource['owningOrganizationKey']+','+line.strip()+','+resource['key']+','+resource['title']+','+descVal+','+resource['citation']['text']+','+rightsVal+','+logoVal+','+resource['created']+','+resource['modified']+','+homepageVal
	#out.write(resource['owningOrganizationKey']+','+line.strip()+','+resource['key']+','+resource['title']+','+descVal+','+resource['citation']['text']+','+rightsVal+','+logoVal+','+resource['created']+','+resource['modified']+','+homepageVal+'\n')

	#l = json.dumps(data)

out.close()
infile.close()


#Name vs Display name
