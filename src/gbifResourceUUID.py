#! /usr/bin/env python

#http://www.pythonforbeginners.com/python-on-the-web/how-to-use-urllib2-in-python/

import argparse
import urllib2
import json
import codecs

def queryApi(infile, outfile, missingfile, newId):
    out = codecs.open(outfile, 'w','utf-8')
    #jsonout = codecs.open('outResourceUUID.json', 'w','utf-8')
    infile = codecs.open(infile, 'r','utf-8')
    if not newId:
        print "Using default 200000 for legacy id"
        newId = 200000
    else:
        newId = int(newId)
    #responseResource =urllib2.urlopen('http://api.gbif.org/v0.9/dataset?identifier=14348&identifier_type=GBIF_PORTAL')
    #responseresource= urllib2.urlopen(' http://api.gbif.org/v0.9/organization?identifier=49&identifier_type=GBIF_PORTAL')

    # where do i get providerid??? TODO
    print "owningorganizationkey | legacyid | key | title | description | citationtext | citationrights | logourl | created | modified | homepage | providerid\n"
    out.write("owningorganizationkey | legacyid | key | title | description | citationtext | citationrights | logourl | created | modified | homepage | providerid\n")

    for line in infile:
        dataset_key = line.strip()
        url = 'http://api.gbif.org/v1/dataset/' + dataset_key
        print url
        responseresource = urllib2.urlopen(url)

        # Getting the code
        print "HTTP Response code: ", responseresource.code

        # Get all data
        #html = responseresource.read()
        #print "Get all data: ", html

        print "Resource ID:", line
        parsed = parseResults(responseresource, newId)
        responseStr = parsed['response']
        newId = parsed['newid']
        if responseStr == "":
            missingfile.write(dataset_key + "\n")
        print responseStr
        out.write(responseStr)
    
        #l = json.dumps(data, indent=4, sort_keys=True)
        #jsonout.write( l + "\n")
    out.close()
    infile.close()

def parseResults(responseresource, newId):
    resource = json.load(responseresource)
    responseStr = ""
    if resource.has_key("key"):
        if resource.has_key("publishingOrganizationKey"):
            owningorganizationkey = resource['publishingOrganizationKey'].replace("\n", "\\r").replace("\r", "")
        else:
            owningorganizationkey = ""
        if resource.has_key("key"):
            key = resource['key']
        else:
            key=""
        if resource.has_key("title"):
            title = resource['title'].replace("\n", "\\r").replace("\r", "")
        else:
            title = ""
        if resource.has_key("description"):
            descVal = resource['description'].replace("\n", "\\r").replace("\r", "")
        else:
            descVal = ""
        if resource.has_key("citation"):
            citationText = resource['citation']['text'].replace("\n", "\\r").replace("\r", "")
        else:
            citationText = ""
        if resource.has_key("rights"):
            rightsVal = resource['rights'].replace("\n", "\\r").replace("\r", "")
        else:
            rightsVal = ""
        if resource.has_key("logoUrl"):
            logoVal = resource['logoUrl']
        else:
            logoVal = ""

        createdVal = resource['created']
        modifiedVal = resource['modified']

        if resource.has_key("homepage"):
            homepageVal = resource['homepage']
        else:
            homepageVal = ""
        if resource.has_key("identifiers") and len(resource['identifiers'])>0 and resource['identifiers'][0]['type']=='GBIF_PORTAL':
            legacyid = resource['identifiers'][0]['identifier']
        else:
            legacyid = str(newId)
            newId += 1

        responseStr = owningorganizationkey  + '|' + legacyid + '|' + key + '|' + title + '|' + descVal + '|' + citationText + '|' + rightsVal + '|' + logoVal + '|' + createdVal + '|' + modifiedVal + '|' + homepageVal + "|\n"
    return {'response':responseStr, 'newid':newId}


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-i", "--infile", help = "path to the file containing ids to query", required = True)
    parser.add_argument("-o", "--outfile", help = "path to the file which will be written", required = True)
    parser.add_argument("-m", "--missingfile", help = "path to the file which will be written containing datasets with no data", required = True)
    parser.add_argument("-n", "--newid", help = "number to use as the starting point for items without a legacy id")
    args = parser.parse_args()
    print args.infile
    print args.outfile
    queryApi(args.infile, args.outfile, args.missingfile, args.newid)
