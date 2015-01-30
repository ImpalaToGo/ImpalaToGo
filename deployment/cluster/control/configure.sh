echo "Configuring ImpalaToGo"

read -p "Please enter your access key " ACCESS_KEY
read -p "Please enter your secret key " SECRET_KEY
read -p "Please enter your default s3 backet" S3_BACKET


read -p "Is it master ? y/n " IsMaster

if [ "$IsMaster" == "n" ]; then
read -p "Please enter DNS of Master" MASTER_DNS


else
MASTER_DNS=localhost
fi

#get script path
SCRIPT=$(readlink -f "$0")
SCRIPTPATH=$(dirname "$SCRIPT")

$SCRIPTPATH/attachToCluster.sh $ACCESS_KEY $SECRET_KEY $MASTER_DNS $S3_BACKET
