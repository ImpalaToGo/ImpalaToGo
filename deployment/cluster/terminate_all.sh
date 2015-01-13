#!/bin/bash
for cluster in `ls /var/cache/impala2go/clusters/`;
do 
	./terminate_cluster.sh $cluster
done
