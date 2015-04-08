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
BASE_DIR=/root/ImpalaToGo/

shift 3
SSH_PARAMS="-i $KEY"
REMOTE_TARGET=$USER@$TARGET_HOST
REMOTE_BASE_DIR=/home/$USER/build
#tachyon deployment
TACHYON_DIR=tachyon-0.7.0-SNAPSHOT
ssh  -i $KEY $REMOTE_TARGET "mkdir -p $REMOTE_BASE_DIR/tachyon/"
ssh  -i $KEY $REMOTE_TARGET "mkdir -p $REMOTE_BASE_DIR/tachyon/core/target/"
ssh  -i $KEY $REMOTE_TARGET "mkdir -p $REMOTE_BASE_DIR/tachyon/core/src/main/webapp/"
ssh  -i $KEY $REMOTE_TARGET "mkdir -p $REMOTE_BASE_DIR/tachyon/client/target/"
ssh  -i $KEY $REMOTE_TARGET "mkdir -p $REMOTE_BASE_DIR/tachyon/assembly/target/"
ssh  -i $KEY $REMOTE_TARGET "mkdir -p $REMOTE_BASE_DIR/tachyon/conf/"
ssh  -i $KEY $REMOTE_TARGET "mkdir -p $REMOTE_BASE_DIR/tachyon/bin/"
ssh  -i $KEY $REMOTE_TARGET "mkdir -p $REMOTE_BASE_DIR/tachyon/libexec/"

scp -rp -i $KEY $BASE_DIR/thirdparty/$TACHYON_DIR/core/target/*.jar $REMOTE_TARGET:$REMOTE_BASE_DIR/tachyon/core/target/
scp -rp -i $KEY $BASE_DIR/thirdparty/$TACHYON_DIR/core/src/main/webapp/* $REMOTE_TARGET:$REMOTE_BASE_DIR/tachyon/core/src/main/webapp/
scp -rp -i $KEY $BASE_DIR/thirdparty/$TACHYON_DIR/client/target/*.jar $REMOTE_TARGET:$REMOTE_BASE_DIR/tachyon/client/target/
scp -rp -i $KEY $BASE_DIR/thirdparty/$TACHYON_DIR/assembly/target/*.jar $REMOTE_TARGET:$REMOTE_BASE_DIR/tachyon/assembly/target/

scp -rp -i $KEY $BASE_DIR/thirdparty/$TACHYON_DIR/conf/* $REMOTE_TARGET:$REMOTE_BASE_DIR/tachyon/conf/
scp -rp -i $KEY $BASE_DIR/thirdparty/$TACHYON_DIR/bin/* $REMOTE_TARGET:$REMOTE_BASE_DIR/tachyon/bin/
scp -rp -i $KEY $BASE_DIR/thirdparty/$TACHYON_DIR/libexec/* $REMOTE_TARGET:$REMOTE_BASE_DIR/tachyon/libexec/

#impala deployment
scp -rp -i $KEY $BASE_DIR/llvm-ir/* $REMOTE_TARGET:$REMOTE_BASE_DIR/llvm-ir/

scp -rp -i $KEY $BASE_DIR/be/build/debug/catalog/catalogd $REMOTE_TARGET:$REMOTE_BASE_DIR/be/
scp -rp -i $KEY $BASE_DIR/be/build/debug/statestore/statestored   $REMOTE_TARGET:$REMOTE_BASE_DIR/be/

#thirdparty deployment
scp -rp -i $KEY $BASE_DIR/thirdparty/thrift-0.9.0/build/*  $REMOTE_TARGET:$REMOTE_BASE_DIR/thirdparty/thrift/build/

scp  -i $KEY $BASE_DIR/bin/set-pythonpath.sh $REMOTE_TARGET:$REMOTE_BASE_DIR/bin/

#shell deployment
scp -rp  -i $KEY $BASE_DIR/shell/* $REMOTE_TARGET:$REMOTE_BASE_DIR/shell/

#backend
scp  -i $KEY $BASE_DIR/be/build/debug/service/* $REMOTE_TARGET:$REMOTE_BASE_DIR/be

#webap
ssh  -i $KEY $REMOTE_TARGET "mkdir -p $REMOTE_BASE_DIR/www/"
scp -i $KEY $BASE_DIR/www/* $REMOTE_TARGET:$REMOTE_BASE_DIR/www

#frontend
ssh  -i $KEY $REMOTE_TARGET "mkdir -p $REMOTE_BASE_DIR/fe/"
scp  -i $KEY $BASE_DIR/fe/target/impala-frontend-0.1-SNAPSHOT.jar $REMOTE_TARGET:$REMOTE_BASE_DIR/fe
scp  -i $KEY  ${IMPALA_HOME}/shell/*  $REMOTE_TARGET:$REMOTE_BASE_DIR/shell/

#jars dependencies deployment
ssh  -i $KEY $REMOTE_TARGET "mkdir -p $REMOTE_BASE_DIR/fe/dependency/"
for jar in $(ls ${IMPALA_HOME}/fe/target/dependency/*.jar); do
 scp  -i $KEY $jar $REMOTE_TARGET:$REMOTE_BASE_DIR/fe/dependency/
done

#boost deployment
scp  -i $KEY /usr/lib/x86_64-linux-gnu/libboost_thread.so.1.54.0  $REMOTE_TARGET:$REMOTE_BASE_DIR/lib
scp  -i $KEY /usr/lib/x86_64-linux-gnu/libboost_regex.so.1.54.0  $REMOTE_TARGET:$REMOTE_BASE_DIR/lib
scp  -i $KEY /usr/lib/x86_64-linux-gnu/libboost_date_time.so.1.54.0  $REMOTE_TARGET:$REMOTE_BASE_DIR/lib
scp  -i $KEY /usr/lib/x86_64-linux-gnu/libboost_filesystem.so.1.54.0  $REMOTE_TARGET:$REMOTE_BASE_DIR/lib
scp  -i $KEY /usr/lib/x86_64-linux-gnu/libboost_system.so.1.54.0  $REMOTE_TARGET:$REMOTE_BASE_DIR/lib

#libicu deployment
scp  -i $KEY /usr/lib/x86_64-linux-gnu/libicuuc.so.52.1  $REMOTE_TARGET:$REMOTE_BASE_DIR/lib
scp  -i $KEY /usr/lib/x86_64-linux-gnu/libicui18n.so.52.1  $REMOTE_TARGET:$REMOTE_BASE_DIR/lib
scp  -i $KEY /usr/lib/x86_64-linux-gnu/libicudata.so.52.1  $REMOTE_TARGET:$REMOTE_BASE_DIR/lib

#deployment section
scp  -i $KEY $BASE_DIR/deployment/install.sh $REMOTE_TARGET:$REMOTE_BASE_DIR/

