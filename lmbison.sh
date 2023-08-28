#!/bin/bash
#
# This script calls docker commands to create an image, volumes, start a container from
# the image, execute commands in that container, then copy outputs to the local
# filesystem.
#
# Note: this script uses a provided config file to
#   1. create the volumes `data` and `output` (if they do not exist)
#   2. build the image `tutor` (if it does not exist)
#   3. start a container with volumes attached and
#   4. run the chosen command with chosen configuration file
#   5. compute output files in the `output` volume
#   6. copy output files to the host machine

# -----------------------------------------------------------
usage ()
{
    # Commands that are substrings of other commands will match the superstring and
    # possibly print the wrong Usage string.
    config_required=(
        "resolve"
        "chunk_large_file"
        "annotate"
        "summarize"
        "aggregate"
        "check_counts"
        "heat_matrix"
        "pam_stats"
    )
    echo ""
    echo "Usage: $0 <cmd> "
    echo "   or:  $0 <cmd>  <parameters_file>"
    echo ""
    echo "This script creates an environment for an lmbison command to be run with "
    echo "user-configured arguments in a docker container."
    echo "the <cmd> argument can be one of:"
    for i in "${!COMMANDS[@]}"; do
        if [[ " ${config_required[*]} " =~  ${COMMANDS[$i]}  ]] ; then
            echo "      ${COMMANDS[$i]}   <parameters_file>"
        else
            echo "      ${COMMANDS[$i]}"
        fi
    done
    echo "and the <parameters_file> argument must be the full path to a JSON file"
    echo "containing command-specific arguments"
    echo ""
    exit 0
}


# -----------------------------------------------------------
set_defaults() {
    IMAGE_NAME="lmbison"
    CONTAINER_NAME="lmbison_container"
    # Relative host config directory mapped to container config directory
    VOLUME_MOUNT="/volumes/bison"
    # Docker volumes
    IN_VOLUME="input"
    CONFIG_VOLUME="config"
    # bind mount
    BIG_DATA_VOLUME="big_data"

    VOLUME_SAVE_LABEL="saveme"
    VOLUME_DISCARD_LABEL="discard"


    if [ ! -z "$HOST_CONFIG_FILE" ] ; then
        if [ ! -f "$HOST_CONFIG_FILE" ] ; then
            echo "File $HOST_CONFIG_FILE does not exist"
            exit 0
        else
            # Replace host directory with docker container path in config file argument
            host_dir="data/$CONFIG_VOLUME"
            container_dir="$VOLUME_MOUNT/$CONFIG_VOLUME"
            CONTAINER_CONFIG_FILE=$(echo $HOST_CONFIG_FILE | sed "s:^$host_dir:$container_dir:g")
            echo "CONTAINER_CONFIG_FILE is $CONTAINER_CONFIG_FILE"
        fi
    fi

    LOG=/tmp/$CMD.log
    if [ -f "$LOG" ] ; then
        /usr/bin/rm "$LOG"
    fi
    touch "$LOG"
}


# -----------------------------------------------------------
build_image_fill_data() {
    # Build and name an image from Dockerfile in this directory
    # This build also populates the data and env volumes
    image_count=$(docker image ls | grep $IMAGE_NAME |  wc -l )
    if [ $image_count -eq 0 ]; then
        docker build . -t $IMAGE_NAME
    else
        echo " - Image $IMAGE_NAME is already built"  | tee -a "$LOG"
    fi
}


# -----------------------------------------------------------
create_volumes() {
    # Create named RO input volumes for use by any container
    # Small input data, part of repository
    input_vol_exists=$(docker volume ls | grep $IN_VOLUME | wc -l )
    if [ "$input_vol_exists" == "0" ]; then
        echo " - Create volume $IN_VOLUME"  | tee -a "$LOG"
        docker volume create --label=$VOLUME_DISCARD_LABEL $IN_VOLUME
    else
        echo " - Volume $IN_VOLUME is already created"  | tee -a "$LOG"
    fi
    # Small config data, part of repository
    config_vol_exists=$(docker volume ls | grep $CONFIG_VOLUME | wc -l )
    if [ "$config_vol_exists" == "0" ]; then
        echo " - Create volume $CONFIG_VOLUME"  | tee -a "$LOG"
        docker volume create --label=$VOLUME_DISCARD_LABEL $CONFIG_VOLUME
    else
        echo " - Volume $CONFIG_VOLUME is already created"  | tee -a "$LOG"
    fi
}


# -----------------------------------------------------------
#docker run -td --name tutor_container -v data:/volumes/data:ro -v env:/volumes/env: -v output:/volumes/output tutor bash
start_container() {
    # Find running container
    container_count=$(docker ps | grep $CONTAINER_NAME |  wc -l )
    if [ "$container_count" -ne 1 ]; then
        build_image_fill_data
        # Option string for volumes
        # TODO: after debugging, add read-only back to data volume
        vol_opts="--volume ${IN_VOLUME}:${VOLUME_MOUNT}/${IN_VOLUME} \
                  --volume ${CONFIG_VOLUME}:${VOLUME_MOUNT}/${CONFIG_VOLUME}"
        # Option string for bind mount
        bind_src="source=/home/astewart/git/bison/data/${BIG_DATA_VOLUME}"
        bind_target="target=${VOLUME_MOUNT}/${BIG_DATA_VOLUME}"
        bind_opts="--mount type=bind,${bind_src},${bind_target}"

        # Start the container, leaving it up
        echo " - Start container $CONTAINER_NAME from $IMAGE_NAME" | tee -a "$LOG"
        docker run -td --name ${CONTAINER_NAME}  ${vol_opts}  ${bind_opts} ${IMAGE_NAME}  bash
    else
        echo " - Container $CONTAINER_NAME is running" | tee -a "$LOG"
    fi
}


# -----------------------------------------------------------
execute_process() {
    start_container
    # Command to execute in container; tools installed as executables in /usr/local/bin
#    command="python3 ${command_path}/process_gbif.py ${CMD} --config_file=${CONTAINER_CONFIG_FILE}"
#    # or run python command from downloaded repo
#    command="python3 ${command_path}/${CMD}.py --config_file=${CONTAINER_CONFIG_FILE}"
    echo " - Execute '${command}' on container $CONTAINER_NAME" | tee -a "$LOG"
    # Run the command in the container
    docker exec -it ${CONTAINER_NAME} ${command}
}


# -----------------------------------------------------------
save_outputs() {
    # TODO: determine if bind-mount is more efficient than this named-volume
    start_container
    # Copy container output directory to host (no wildcards)
    # If directory does not exist, create, then add contents
    echo " - Copy outputs from $OUT_VOLUME to $IN_VOLUME" | tee -a "$LOG"
    docker cp ${CONTAINER_NAME}:${VOLUME_MOUNT}/${OUT_VOLUME}  ./${IN_VOLUME}/
}


# -----------------------------------------------------------
remove_container() {
    # Find container, running or stopped
    container_count=$(docker ps -a | grep $CONTAINER_NAME |  wc -l )
    if [ $container_count -eq 1 ]; then
        echo " - Stop container $CONTAINER_NAME" | tee -a "$LOG"
        docker stop $CONTAINER_NAME
        echo " - Remove container $CONTAINER_NAME" | tee -a "$LOG"
        docker container rm $CONTAINER_NAME
    else
        echo " - Container $CONTAINER_NAME is not running" | tee -a "$LOG"
    fi
}

# -----------------------------------------------------------
remove_image() {
    remove_container
    # Delete image and volumes
    image_count=$(docker image ls | grep $IMAGE_NAME |  wc -l )
    if [ $image_count -eq 1 ]; then
        echo " - Remove image $IMAGE_NAME" | tee -a "$LOG"
        docker image rm $IMAGE_NAME
    else
        echo " - Image $IMAGE_NAME does not exist" | tee -a "$LOG"
    fi
}

# -----------------------------------------------------------
remove_volumes() {
    remove_container
    echo " - Remove volumes $IN_VOLUME $CONFIG_VOLUME" | tee -a "$LOG"
    input_vol_exists=$(docker volume ls | grep $IN_VOLUME | wc -l )
    if [ "$input_vol_exists" != "0" ]; then
        docker volume rm $IN_VOLUME
    fi
    conf_vol_exists=$(docker volume ls | grep $CONFIG_VOLUME | wc -l )
    if [ "$conf_vol_exists" != "0" ]; then
        docker volume rm $CONFIG_VOLUME
    fi
}

# -----------------------------------------------------------
list_output_volume_contents() {
    # Find an image, start it with output volume, check contents
    start_container
    echo " - List output volume contents $CONTAINER_NAME" | tee -a "$LOG"
    docker exec -it $CONTAINER_NAME ls -lahtr ${VOLUME_MOUNT}/${OUT_VOLUME}
}


# -----------------------------------------------------------
list_all_volume_contents() {
    # Find an image, start it with output volume, check contents
    start_container
    echo " - List volume contents $CONTAINER_NAME" | tee -a "$LOG"
    docker exec -it $CONTAINER_NAME ls -lahtr ${VOLUME_MOUNT}
    echo "    - Volume $CONFIG_VOLUME" | tee -a "$LOG"
    docker exec -it $CONTAINER_NAME ls -lahtr ${VOLUME_MOUNT}/${CONFIG_VOLUME}
    echo "    - Volume $IN_VOLUME" | tee -a "$LOG"
    docker exec -it $CONTAINER_NAME ls -lahtr ${VOLUME_MOUNT}/${IN_VOLUME}
    echo "    - Volume $BIG_DATA_VOLUME" | tee -a "$LOG"
    docker exec -it $CONTAINER_NAME ls -lahtr ${VOLUME_MOUNT}/${BIG_DATA_VOLUME}
}


# -----------------------------------------------------------
open_container_shell() {
    # Find an image, start it with output volume, check contents
    start_container
    echo " - Connect to $CONTAINER_NAME" | tee -a "$LOG"
    docker exec -it $CONTAINER_NAME bash
}


# -----------------------------------------------------------
time_stamp () {
    echo "$1" $(/bin/date) | tee -a "$LOG"
}


# -----------------------------------------------------------
####### Main #######
COMMANDS=(
# No command-line parameters
"build_all"  "cleanup"  "cleanup_all"
"list_commands" "list_outputs"  "list_volumes"
# Need configuration file parameter
"resolve"
"chunk_large_file"
"annotate"
"summarize"
"aggregate"
"check_counts"
"heat_matrix"
"pam_stats"
)


CMD=$1
HOST_CONFIG_FILE=$2
arg_count=$#
command_path=/git/bison/

set_defaults
time_stamp "# Start"
echo ""

# Arguments: none
if [ $arg_count -eq 0 ]; then
    usage
# Arguments: command
elif [ $arg_count -eq 1 ]; then
    if [ "$CMD" == "cleanup" ] ; then
        remove_container
        remove_image
        docker volume prune --filter "label=discard"
    elif [ "$CMD" == "cleanup_all" ] ; then
        remove_container
        remove_image
        remove_volumes
    elif [ "$CMD" == "open" ] ; then
        open_container_shell
    elif [ "$CMD" == "list_commands" ] ; then
        usage
    elif [ "$CMD" == "list_outputs" ] ; then
        list_output_volume_contents
    elif [ "$CMD" == "list_volumes" ] ; then
        list_all_volume_contents
    elif [ "$CMD" == "build_all" ] ; then
        remove_container
        remove_image
        remove_volumes
        echo "System build will take approximately 5 minutes ..." | tee -a "$LOG"
        create_volumes
        build_image_fill_data
    elif [ "$CMD" == "test" ]; then
        list_output_volume_contents
        remove_container
        open_container_shell
        remove_container
    else
        usage
    fi
# Arguments: command, config file
else
    echo "*** arg_count > 1; command $CMD ***" | tee -a "$LOG"
    if [[ " ${COMMANDS[*]} " =~  ${CMD}  ]]; then
        create_volumes
        echo "*** create_volumes ***" | tee -a "$LOG"
        build_image_fill_data
        echo "*** build_image_fill_data ***" | tee -a "$LOG"
        start_container
        echo "*** start_container ***" | tee -a "$LOG"
        execute_process
        echo "*** execute_process ***" | tee -a "$LOG"
        # save_outputs
        # echo "*** save_outputs ***" | tee -a "$LOG"
        remove_container
        echo "*** remove_container ***" | tee -a "$LOG"
    else
        echo "Unrecognized command: $CMD"
        usage
    fi
fi

time_stamp "# End"
