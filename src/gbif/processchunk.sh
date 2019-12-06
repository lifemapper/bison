#!/bin/bash
#
# This script 
# sed -e "1,${start}d;${idx}q" $pth/$base.txt > $pth/${base}_lines_$start-$stop.csv


#time python /state/partition1/workspace/bison/src/gbif/gbif2bison.py \
#     --start_line=$start --stop_line=$stop

# sed -e '1,5000d;10000q' occurrence.txt > occurrence_lines_5000-10000.csv


usage () 
{
    echo "Usage: $0 <SUBDIR>  <INFILE_BASENAME>"
    echo "This script is run on a subset of a GBIF occurrence download in Darwin Core format"
    echo "It will:"
    echo "     - process the data into BISON-acceptable CSV format "
    echo "       for loading into a database and/or SOLR index"
    echo "   "
    echo "The script can be run at any time to ..."
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
     	TimeStamp "Process ${DATANAME} with step ${stp}" >> $LOG
       	time python $PROCESS_CSV $INFILE --step=$stp >> $LOG
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
if [ $# -ne 2 ]; then
    usage
    exit 0
fi 


set_defaults $1 $2
TimeStamp "# Start"

c=1
while [[ $c -le $count ]]
do 
	stop=$((start+inc))
	convert_data $start $stop
	start=$stop

	TimeStamp "Loop $c"
	let c=c+1
done
TimeStamp "# End"
################################# Main #################################


 
