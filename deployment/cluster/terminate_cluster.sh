#!/bin/bash
BATCH_ID=$1

. resize.config
take_lock
function cleanup_cluster_residue(){
	rm -f $BATCH_ID
	rm -f $KNOWN_CLUSTERS_DIR/$BATCH_ID
	rm -rf $CLUSTER_VAR_DIR
	rm -f $TMP_FILE
}

if [ -z "$AWS_CMD" ];
then
	AWS_CMD="aws ec2"
fi
set -x
$AWS_CMD describe-instances $DRY_RUN --filters Name=client-token,Values=$BATCH_ID|tee $TEMP_FILE

INSTANCE_IDS=$(grep $BATCH_ID <$TEMP_FILE|cut -f8)
echo instance list: $INSTANCE_IDS
if [ -z "$INSTANCE_IDS" ];
then
	echo Cluster $BATCH_ID has no instances
	echo Either wrong ID provided, all instances were terminated by other provess or there is residue from other cluster-related scripts
	cleanup_cluster_residue
	release_lock
	exit 1
fi
for instance in $INSTANCE_IDS; do
	echo found instance: $instance
done
echo Terminating instances: $INSTANCE_IDS
$AWS_CMD terminate-instances --instance-ids $INSTANCE_IDS
echo terminate command sent, waiting termination
$AWS_CMD wait instance-terminated --instance-ids $INSTANCE_IDS
echo instances terminated, cleanup local resources
cleanup_cluster_residue
release_lock
echo Done
