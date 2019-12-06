#!/bin/bash
#
# linecount of /tank/data/bison/2019/Terr/occurrence.txt = 72890559
# bash /state/partition1/git/bison/src/gbif/chunkfile.sh Terr 72890559 10000000
#
# Example command
# sed -e '1,5000d;10000q' occurrence.txt > occurrence_lines_5000-10000.csv


usage () 
{
    echo "Usage: $0 <SUBDIR>  <TOTAL_LINECOUNT>  <LINECOUNT_PER_CHUNK>"
    echo "This script is run on a large GBIF occurrence download named"
    echo "occurrence.txt in the <SUBDIR> subdirectory of /tank/data/bison/2019/"
    echo "  and chunk it into files of LINECOUNT_PER_CHUNK lines or less"
    echo "   "
}

### Set variables 
set_defaults() {
    dataname=occurrence
    pth=/tank/data/bison/2019
    SUBDIR=$1
    THISPTH=$pth/$SUBDIR
    INFILE=$THISPTH/$dataname.txt
    echo 'Input = ' $INFILE
	
    THISNAME=`/bin/basename $0`
    LOG=$THISPTH/$THISNAME.log
    # Append to existing logfile
    touch $LOG
}

### Chunk into smaller file
chunk_data() {
    start=$1
    stop=$2
    postfix=_lines_$start-$stop
	
    outfile=$THISPTH/$dataname$postfix.csv
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

# bash /state/partition1/workspace/bison/src/gbif/chunkfile.sh 7000000 1

 
