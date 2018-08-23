#!/bin/bash
#
# This script 
# sed -e "1,${start}d;${idx}q" $pth/$base.txt > $pth/${base}_lines_$start-$stop.csv


#time python /state/partition1/workspace/bison/src/gbif/gbif2bison.py \
#     --start_line=$start --stop_line=$stop

# sed -e '1,5000d;10000q' occurrence.txt > occurrence_lines_5000-10000.csv


usage () 
{
    echo "Usage: $0 <START_LINE>  <ITERATIONS>"
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
    start=$1
    count=$2
    inc=1000000
    pth=/state/partition1/data/bison/terr
    base=occurrence
    outbase=outBison

    echo $pth/$base$postfix.csv
	
    PROCESS_CSV=/state/partition1/workspace/bison/src/gbif/gbif2bison.py

    THISNAME=`/bin/basename $0`

    LOG=$pth/$THISNAME.log
    # Append to existing logfile
    touch $LOG
}


### Convert some data 
convert_data() {
    start=$1
    stop=$2
    postfix=_lines_$start-$stop
	
    infile=$pth/$base$postfix.csv
    if [ -s $infile ]; then
    	LogStuff "${infile} exists"
    else
    	LogStuff "${infile} is empty or does not exist"  
    fi

    outfile=$pth/$outbase$postfix.csv
    if [ -s $outfile ]; then
	LogStuff "${outfile} exists"
    else
	LogStuff "${outfile} does not exist"
	TimeStamp "process into ${outfile}" >> $LOG
	time python $PROCESS_CSV >> $LOG
	TimeStamp ""
    fi
}

TimeStamp () {
    echo $1 `/bin/date`
    echo $1 `/bin/date` >> $LOG
}

LogStuff () {
    echo $1
    echo $1 >> $LOG
}

####### Main #######
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

# bash /state/partition1/workspace/bison/src/gbif/chunkfile.sh 7000000 1

 
