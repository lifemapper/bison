pth=terr
base=occurrence
outbase=outBison
inc=1000000
PROCESS_CSV=/state/partition1/workspace/bison/src/gbif/gbif2bison.py

for start in {1000000..46000000..1000000}; do
    stop=$((start+inc))
    infile=${base}_lines_$start-$stop.csv
    outfile=${outbase}_lines_$start-$stop.csv
    if [[ -s $infile && ! -f $outfile ]]; then
        echo $infile exists, process $outfile 
        time python $PROCESS_CSV --start_line=$start --stop_line=$stop
    else
        echo $infile is empty or $outfile exists
    fi
    start=$idx
done



#sed -e "1,${start}d;${idx}q" $pth/$base.txt > $pth/${base}_lines_$start-$stop.csv


#time python $PROCESS_CSV  --start_line=$start --stop_line=$stop

# sed -e '1,5000d;10000q' occurrence.txt > occurrence_lines_5000-10000.csv
#time python $PROCESS_CSV --start_line=$start --stop_line=$stop
#sed -e "1,${start}d;${stop}q" $base.txt > $outfile