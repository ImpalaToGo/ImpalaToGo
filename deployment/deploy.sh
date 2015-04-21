#cdh5 target

function usage(){
	echo deploy ImpalaToGo artifacts to REMOTE_HOST using USERNAME and SSH_KEY_FILE
	echo Usage:
	echo "\t$0 REMOTE_HOST USERNAME SSH_KEY_FILE"
}
if [ -z "$3" ];
then
	usage
	exit 1
fi

TARGET_HOST=$1
USER=$2
#authorization 
KEY=$3
if [ -d $IMPALA_HOME ];
then
BASE_DIR=$IMPALA_HOME
else
BASE_DIR=/root/ImpalaToGo
fi

shift 3
SSH_PARAMS="-i $KEY"
REMOTE_TARGET=$USER@$TARGET_HOST
REMOTE_BASE_DIR=/dbcache/
echo -n Copying build results to target...
scp -v $SSH_PARAMS $BASE_DIR/build.tgz $REMOTE_TARGET:$REMOTE_BASE_DIR
echo Done
echo -n Unpacking build results on target...
#TODO: Cleanup target directory
ssh $SSH_PARAME $REMOTE_TARGET tar -xzf $REMOTE_BASE_DIR/build.tgz
echo Done
echo -n Running install script on target...
#TODO: Remove previous version before installing
ssh $SSH_PARAME $REMOTE_TARGET /home/$USER/build/deployment/install.sh
echo Done
