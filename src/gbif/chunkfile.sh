#!/bin/bash
#
# This script 
# sed -e "1,${start}d;${idx}q" $pth/$base.txt > $pth/${base}_lines_$start-$stop.csv


#time python /state/partition1/workspace/bison/src/gbif/gbif2bison.py \
#     --start_line=$start --stop_line=$stop

# sed -e '1,5000d;10000q' occurrence.txt > occurrence_lines_5000-10000.csv


usage () 
{
    echo "Usage: $0 <SUBDIR>  <LINECOUNT_PER_CHUNK>"
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
    LINECOUNT=$2
    THISPTH=$pth/$SUBDIR/
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
	
    outfile=$THISPTH/$base$postfix.csv
    if [ -s $outfile ]; then
    	TimeStamp "${outfile} exists"
    else
    	TimeStamp "${outfile} is empty or does not exist"  
    	TimeStamp "chunk into ${outfile}" >> $LOG
    	sed -e "1,${start}d;${stop}q" $INFILE > $outfile
    	TimeStamp ""
    fi
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
	chunk_data $start $stop
	start=$stop

	TimeStamp "Loop $c"
	let c=c+1
done
TimeStamp "# End"
################################# Main #################################

# bash /state/partition1/workspace/bison/src/gbif/chunkfile.sh 7000000 1

 
