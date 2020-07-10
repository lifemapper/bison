#!/bin/bash
#
# [astewart@badenov CA_USTerr_gbif]$ wc -l occurrence.txt 
# 74804557 occurrence.txt
# bash /state/partition1/git/bison/src/gbif/chunkfile.sh occurrence.txt 74804557 10000000
#
# [astewart@badenov US_gbif]$ wc -l occurrence.txt 
# 468177858 occurrence.txt
# bash /state/partition1/git/bison/src/gbif/chunkfile.sh occurrence.txt 468177858 50000000

#
# Example command
# sed -e '1,5000d;10000q' occurrence.txt > occurrence_lines_5000-10000.csv


usage () 
{
    echo "Usage: $0 <DATAFILE>  <TOTAL_LINECOUNT>  <LINECOUNT_PER_CHUNK>"
    echo "This script is run on a large delimited text file named <DATAFILE> "
    echo "  to chunk it into files of LINECOUNT_PER_CHUNK lines or less"
    echo "   "
}

### Set variables 
set_defaults() {
    INFILE=$1
    fname=`/usr/bin/basename $INFILE`
    pth=`/usr/bin/dirname $INFILE`
    dataname=${fname%.*}
    
    echo 'Input = ' $INFILE
	
    THISNAME=`/bin/basename $0`
    LOG=$pth/$THISNAME.log
    # Append to existing logfile
    touch $LOG
}

### Chunk into smaller file
chunk_data() {
    start=$1
    stop=$2
    postfix=_lines_$start-$stop
	
    outfile=$pth/$dataname$postfix.csv
    echo 'Output = ' $outfile
    
    if [ -s $outfile ]; then
    	TimeStamp "${outfile} exists"
    else
     	TimeStamp "chunk into ${outfile}" >> $LOG
     	sedstr=`echo "1,${start}d;${stop}q"`
     	echo 'sed string = '  $sedstr
     	sed -e $sedstr $INFILE > $outfile
        echo ''
        echo '' >> $LOG
    fi
}

### Log progress
TimeStamp () {
    echo $1 `/bin/date`
    echo $1 `/bin/date` >> $LOG
}


################################# Main #################################
if [ $# -ne 3 ]; then
    usage
    exit 0
fi 

set_defaults $1
linecount=$2
inc=$3

TimeStamp "# Start"
start=1

while [[ $start -lt linecount ]]
do 
	stop=$((start+inc))
	if [[ $stop -ge linecount ]]; then
	    stop=$linecount
	fi
	echo 'Start/stop = '  $start  $stop
	chunk_data $start $stop
	start=$stop
done
TimeStamp "# End"
################################# Main #################################

# bash /state/partition1/workspace/bison/src/gbif/chunkfile.sh /tank/data/bison/2019/Terr/occurrence.txt 7000000 1

# bash /state/partition1/workspace/bison/src/gbif/chunkfile.sh /tank/data/bison/2019/ancillary/bison.csv 101996788 10000000 
 
