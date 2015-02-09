#!/bin/bash
#set RY_RUN just to run nothing on amazon ocasionally
DRY_RUN="--dry-run"
#check if we have root permissions
ROOT=$(whoami)

if [ ! "xroot" == "x$ROOT" ];
then
	echo $0 requires root permissions to run.
	echo please run "sudo $0"
	exit 1
fi

. resize.config

function prepare_folder(){
	local FOLDER=$1
	mkdir -p $FOLDER
	chmod -R o+w $FOLDER
	chmod -R o+r $FOLDER
	chmod o+x $FOLDER
}

prepare_folder $LOG_DIR
prepare_folder $IMPALA_TO_GO_CACHE
prepare_folder $KNOWN_CLUSTERS_DIR
prepare_folder $CLUSTER_VAR_BASE
prepare_folder $LOCK_DIR
