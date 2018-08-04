pth=terr
base=occurrence
inc=1000000

start=1
for start in {1000000..46000000..1000000}; do 
    stop=$((start+inc))
    if [ $start -gt 1 ]; then
        outfile=${base}_lines_$start-$stop.csv
        if [ -s $outfile ]; then
        	echo $outfile exists
        else
        	echo $outfile is empty or does not exist
        	sed -e "1,${start}d;${stop}q" $base.txt > $outfile
        fi
        echo ''
    fi
    start=$idx
done

start=1
stop=1000000

start=$stop
stop=$((start+inc))
echo $pth/${base}_lines_$start-$stop.csv
sed -e "1,${start}d;${idx}q" $pth/$base.txt > $pth/${base}_lines_$start-$stop.csv


time python /state/partition1/workspace/bison/src/gbif/gbif2bison.py \
     --start_line=$start --stop_line=$stop

# sed -e '1,5000d;10000q' occurrence.txt > occurrence_lines_5000-10000.csv