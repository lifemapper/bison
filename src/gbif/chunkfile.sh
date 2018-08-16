<<<<<<< HEAD
#!/bin/bash
#
# This script 
# sed -e "1,${start}d;${idx}q" $pth/$base.txt > $pth/${base}_lines_$start-$stop.csv


#time python /state/partition1/workspace/bison/src/gbif/gbif2bison.py \
#     --start_line=$start --stop_line=$stop

# sed -e '1,5000d;10000q' occurrence.txt > occurrence_lines_5000-10000.csv
# bash /state/partition1/workspace/bison/src/gbif/chunkfile.sh 4000000 2
# -rw-rw-r-- 1 astewart astewart 429354250 Aug  3 21:17 outBison_lines_1000000-2000000.csv
# -rw-rw-r-- 1 astewart astewart 439149776 Aug  3 18:06 outBison_lines_1-1000000.csv
# -rw-rw-r-- 1 astewart astewart 431252987 Aug  4 00:09 outBison_lines_2000000-3000000.csv
# -rw-rw-r-- 1 astewart astewart 432123347 Aug  4 00:21 outBison_lines_3000000-4000000.csv
# -rw-rw-r-- 1 astewart astewart   3247409 Aug  4 00:26 outBison_lines_4000000-5000000.csv
# -rw-rw-r-- 1 astewart astewart 110767342 Aug  4 17:43 outBison_lines_5000000-6000000.csv
# -rw-rw-r-- 1 astewart astewart 450090225 Aug  5 16:29 outBison_lines_6000000-7000000.csv
# -rw-rw-r-- 1 astewart astewart 286273536 Aug  7 16:00 outBison_lines_7000000-8000000.csv


usage () 
{
    echo "Usage: $0 <START_LINE>  <ITERATIONS>"
    echo "This script is run on a GBIF occurrence download in Darwin Core format"
    echo "It will:"
    echo "     - chunk the original data file into 1 million line files"
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
	
    THISNAME=`/bin/basename $0`

    LOG=$pth/$THISNAME.log
    # Append to existing logfile
    touch $LOG
}


### Split out some data 
chunk_data() {
	start=$1
	stop=$2
	postfix=_lines_$start-$stop
	
    infile=$pth/$base$postfix.csv
    if [ -s $infile ]; then
    	LogStuff "${infile} exists"
    else
    	LogStuff "${infile} is empty or does not exist"  
     	sed -e "1,${start}d;${stop}q" $base.txt > $outfile
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
	chunkk_data $start $stop
	start=$stop

	TimeStamp "Loop $c"
	let c=c+1
done
TimeStamp "# End"
