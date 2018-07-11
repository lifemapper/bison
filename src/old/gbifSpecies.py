#! /usr/bin/env python
import argparse
import urllib
import urllib2
import json
import codecs
import time
from urllib2 import URLError

def queryApi(infile, outfile, missedfile):
    out = codecs.open(outfile, 'w','utf-8')
    missedIds = codecs.open(missedfile, 'w','utf-8')
    infile = codecs.open(infile, 'r','utf-8')

    #http://api.gbif.org/v1/species/2490384/
    retryCnt=0
    respCode=200

    # Write the headers for the import
    out.write("taxon_id | canonicalname | datasetkey\n")

    for line in infile:
        print "http://api.gbif.org/v1/species/"+line.strip()
        try: responsespecies = urllib2.urlopen('http://api.gbif.org/v1/species/'+line.strip())
        except Exception as e:
            respCode=-1
            print 'Error in initial call:'

        # Getting the code
        #respCode=responsespecies.code
        while respCode!=200 and retryCnt<5:
            try: responsespecies = urllib2.urlopen('http://api.gbif.org/v1/species/'+line.strip())
            except URLError as e:
                print 'Error in loop '+str(retryCnt)
                respCode=-1
            retryCnt+=1
            time.sleep(5+retryCnt*5)

        retryCnt=0
        if  respCode!=200:
            missedIds.write(line.strip()+'\n')
            respCode=200
            continue

        retryCnt=0;

        data = json.load(responsespecies)
        print "Length:", len(data)
        #species=data['results'][0]
        species=data
        if species.has_key("canonicalName"):
            nameVal= species['canonicalName']
        else:
            nameVal=""

        print line.strip()+'|'+nameVal+'|'+species['datasetKey']
        out.write(line.strip()+'|'+nameVal+'|'+species['datasetKey']+'\n')

    out.close()
    infile.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-i", "--infile", help = "path to the file containing ids to query", required = True)
    parser.add_argument("-o", "--outfile", help = "path to the file which will be written with species data", required = True)
    parser.add_argument("-m", "--missed", help = "ids which were unable to be queried are written to this file", required = True)
    args = parser.parse_args()
    print args.infile
    print args.outfile
    queryApi(args.infile, args.outfile, args.missed)
