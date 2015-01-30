#!/bin/bash
BATCH_ID=$1
HOST=$2
shift
. resize.config
ssh $SSH_PARAMS ec2-user@$HOST
