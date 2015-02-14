#!/bin/bash 
#parameters are
# $1 access key
# $2 secret key
# $3 DNS of the master 
# $4 S3 backet with data
echo "Attaching to cluster"

ACCESS_KEY=$1
SECRET_KEY=$2
MASTER_DNS=$3
S3_BACKET=$4

#get script path
SCRIPT=$(readlink -f "$0")
SCRIPTPATH=$(dirname "$SCRIPT")

eval sed -e 's/ACCESS_KEY/${ACCESS_KEY}/g' -e 's/SECRET_KEY/${SECRET_KEY}/g' -e 's/S3_BACKET/${S3_BACKET}/g'  $SCRIPTPATH/conf/hdfs-site.template > hdfs-site.xml
eval sed -e 's/ACCESS_KEY/${ACCESS_KEY}/g' -e 's/SECRET_KEY/${SECRET_KEY}/g' $SCRIPTPATH/conf/hive-site.template > hive-site.xml
eval sed 's/MASTER_DNS/${MASTER_DNS}/g' $SCRIPTPATH/conf/impala_defaults.template >impala

eval sed -e 's/ACCESS_KEY/${ACCESS_KEY}/g' -e 's/SECRET_KEY/${SECRET_KEY}/g' -e 's/S3_BACKET/${S3_BACKET}/g'  $SCRIPTPATH/conf/core-site.template > core-site.xml
eval sed -e 's/ACCESS_KEY_PLACE/${ACCESS_KEY}/g' -e 's/SECRET_KEY_PLACE/${SECRET_KEY}/g'  -e 's/S3_BACKET_PLACE/${S3_BACKET}/g'  $SCRIPTPATH/conf/resize.template > $SCRIPTPATH/cluster/resize.config

sudo cp hdfs-site.xml /etc/impala/conf
sudo cp hive-site.xml /etc/hive/conf/
sudo cp hive-site.xml /etc/impala/conf/

sudo cp hive-site.xml /etc/alternatives/hive-conf/
sudo cp impala /etc/default/
sudo cp core-site.xml /etc/impala/


rm hdfs-site.xml
rm hive-site.xml
rm impala
rm core-site.xml


$SCRIPTPATH/linkEphimerialDrive.sh
