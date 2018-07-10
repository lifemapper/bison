#http://www.pythonforbeginners.com/python-on-the-web/how-to-use-urllib2-in-python/

import urllib2
import json
import codecs

out = codecs.open('outResource.txt', 'w','utf-8')
infile = codecs.open('inResource.txt', 'r','utf-8')

#responseResource =urllib2.urlopen('http://api.gbif.org/v0.9/dataset?identifier=14348&identifier_type=GBIF_PORTAL')
#responseresource= urllib2.urlopen(' http://api.gbif.org/v0.9/organization?identifier=49&identifier_type=GBIF_PORTAL')
out.write("owningorganization_id|\"legacyid\"|dataset_id|name|description|citation|created|modified|website_url\n")
for line in infile:
        print "http://api.gbif.org/v1/dataset?identifier="+line.strip()+"&identifier_type=GBIF_PORTAL"
    responseresource = urllib2.urlopen('http://api.gbif.org/v0.9/dataset?identifier='+line.strip()+'&identifier_type=GBIF_PORTAL')

    # Getting the code
    print "This gets the code: ", responseresource.code

    # Get all data
    #html = responseresource.read()
    #print "Get all data: ", html

    print "Resource ID:", line
    data = json.load(responseresource)
    if len(data['results']) > 0:
        resource=data['results'][0]

        # Not sure about these next few fields if the mappings are correct
        if resource.has_key("publishingOrganizationKey"):
                owningorganizationkey=resource['publishingOrganizationKey'].replace("\n", "\\n")
        else:
                owningorganizationkey=""
        legacyid = line.strip()
        if resource.has_key("key"):
            provider_id = resource['key'].replace("\n", "\\n")
        else:
            provider_id = ""


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
        print owningorganizationkey + '|' + legacyid + '|'
        print resource['publishingOrganizationKey'].replace("\n", "\\n")+'|'+line.strip()+'|'+resource['key'].replace("\n", "\\n")+'|'+resource['title'].replace("\n", "\\n")+'|'+descVal.replace("\n", "\\n")+'|'+resource['citation']['text'].replace("\n", "\\n")+'|'+rightsVal.replace("\n", "\\n")+'|'+logoVal.replace("\n", "\\n")+'|'+resource['created'].replace("\n", "\\n")+'|'+resource['modified'].replace("\n", "\\n")+'|'+homepageVal.replace("\n", "\\n")
        out.write(resource['publishingOrganizationKey'].replace("\n", "\\n")+'|'+line.strip()+'|'+resource['key'].replace("\n", "\\n")+'|'+resource['title'].replace("\n", "\\n")+'|'+descVal.replace("\n", "\\n")+'|'+resource['citation']['text'].replace("\n", "\\n")+'|'+rightsVal.replace("\n", "\\n")+'|'+logoVal.replace("\n", "\\n")+'|'+resource['created'].replace("\n", "\\n")+'|'+resource['modified'].replace("\n", "\\n")+'|'+homepageVal.replace("\n", "\\n")+'\n')
        
        #l = json.dumps(data)

out.close()
infile.close()


#Name vs Display name
