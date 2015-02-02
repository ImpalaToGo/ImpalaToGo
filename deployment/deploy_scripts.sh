# This script deploys scripts to the target machine
# first param - key
# second parameter - host
KEY=$1
TARGET_HOST=$2
scp -i $KEY cluster/* ec2-user@$TARGET_HOST:/home/ec2-user/cluster/
scp -i $KEY cluster/control/* ec2-user@$TARGET_HOST:/home/ec2-user/
scp -i $KEY cluster/conf/* ec2-user@$TARGET_HOST:/home/ec2-user/conf/

