#!/bin/bash
#

usage ()
{
    echo "Usage: $0 <SUBDIR>  <INFILE_BASENAME_WO_EXT> <TOTAL_LINECOUNT>  <LINECOUNT_PER_CHUNK>"
    echo "This script is run on a subset of a GBIF occurrence download in Darwin Core format"
    echo "It will:"
    echo "     - process the data into BISON-acceptable CSV format "
    echo "       for loading into a database and/or SOLR index"
    echo "   "
}

### Set variables
set_defaults() {
    pth=/tank/data/bison/2019
    SUBDIR=$1
    DATANAME=$2
    THISPTH=$pth/$SUBDIR/
    INFILE=$THISPTH/$DATANAME.txt
    echo 'Input = ' $INFILE

    PROCESS_CSV=/state/partition1/git/bison/gbif/convert2bison.py

    THISNAME=`/bin/basename $0`
    LOG=$THISPTH/$THISNAME.log
    # Append to existing logfile
    touch $LOG
}

### Convert chunk of GBIF data to BISON format
convert_data() {
    stp=1
    while [[ $stp -le 4 ]]
    do
     	TimeStamp "Process ${INFILE} with step ${stp}" >> $LOG
       	# time python $PROCESS_CSV $INFILE --step=$stp >> $LOG
        TimeStamp "Step $stp complete"
        let stp=stp+1
    done
       	TimeStamp ""
}

### Log progress
TimeStamp () {
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
