#!/bin/bash
BATCH_ID=$1

. resize.config
wait_lock
take_lock
aws ec2 describe-instances $DRY_RUN --filters Name=client-token,Values=$BATCH_ID|tee $TEMP_FILE
INSTANCE_IDS=$(grep $BATCH_ID <$TEMP_FILE|cut -f8)
echo instance list: $INSTANCE_IDS
for instance in $INSTANCE_IDS; do
	echo found instance: $instance
done
echo Terminating instances: $INSTANCE_IDS
aws ec2 terminate-instances --instance-ids $INSTANCE_IDS
echo terminate command sent, waiting termination
aws ec2 wait instance-terminated --instance-ids $INSTANCE_IDS
echo instances terminated, cleanup local resources
rm -f $BATCH_ID
rm -f $KNOWN_CLUSTERS_DIR/$BATCH_ID
rm -rf $CLUSTER_VAR_DIR
rm -f $TMP_FILE
release_lock
echo Done
