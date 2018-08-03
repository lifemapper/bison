pth=terr
base=occurrence

start=1
for idx in {0..46000000..1000000}; do 
    if [ $start -gt 1 ]; then
       echo $pth/$base_lines_$start-$idx.csv
       sed -e "1,${start}d;${idx}q" $pth/$base.txt > $pth/$base_lines_$start-$idx.csv
       echo ''
    fi
    start=$idx
done


# sed -e '1,5000d;10000q' occurrence.txt > occurrence_lines_5000-10000.csv