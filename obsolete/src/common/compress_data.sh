time_stamp () {
    echo $1 `/bin/date`
}

set_defaults() {
    BISON_DATA_PATH=/tank/data/bison/2019
    GBIF_US_DIR=US_gbif
    GBIF_TERR_DIR=CA_USTerr_gbif
    BPROV_DIR=provider
}

compress_data() {
    DATASOURCE_DIR=$1

    PTH=$BISON_DATA_PATH/$DATASOURCE_DIR/output/
    FILES=$PTH/occ*csv
    startidx=$((${#PTH} + 2))
    for f in $FILES
        do
            filelen=${#f}
            # filename prefix sometimes differs from package name
            basename=$(echo $f | cut -c$startidx-$filelen | cut -d'.' -f1)
            csvname=$PTH/$basename.csv
            tarname=$PTH/$basename.tar.gz
            if [[ -f $tarname ]]; then
                echo "$basename compressed already ..."
            else
                echo "compress $basename.csv ..."
                echo "target cvzf $basename.tar.gz $basename.csv"
            fi
        done
}


### main ###
set_defaults
time_stamp "# Start"

compress_data $GBIF_TERR_DIR

time_stamp "# End"
