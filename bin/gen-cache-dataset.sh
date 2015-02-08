#!/bin/bash
# $1 output path
# $2 number of files
# $3 size in Kilobytes

for i in `seq 1 $2`;
        do
                dd if=/dev/urandom of=$1/output$i.dat  bs=1024  count=$3
        done