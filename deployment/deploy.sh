TARGET_HOST=ec2-54-167-164-37.compute-1.amazonaws.com
KEY=/root/key/ImpalaToGo.pem

BASE_DIR=/root/ImpalaToGo/

scp -rp -i $KEY /root/ImpalaToGo/llvm-ir/* ec2-user@$TARGET_HOST:/home/ec2-user/build/llvm-ir/

scp -rp -i $KEY /root/ImpalaToGo/be/build/debug/catalog/catalogd ec2-user@$TARGET_HOST:/home/ec2-user/build/be/
scp -rp -i $KEY /root/ImpalaToGo/be/build/debug/statestore/statestored   ec2-user@$TARGET_HOST:/home/ec2-user/build/be/


scp -rp -i $KEY /root/ImpalaToGo/thirdparty/thrift-0.9.0/build/*  ec2-user@$TARGET_HOST:/home/ec2-user/build/thirdparty/thrift/build/

scp  -i $KEY /root/ImpalaToGo/bin/set-pythonpath.sh ec2-user@$TARGET_HOST:/home/ec2-user/build/bin/
scp -rp  -i $KEY /root/ImpalaToGo/shell/* ec2-user@$TARGET_HOST:/home/ec2-user/build/shell/
scp  -i $KEY /root/ImpalaToGo/be/build/debug/service/* ec2-user@$TARGET_HOST:/home/ec2-user/build/be

scp  -i $KEY /root/ImpalaToGo/fe/target/impala-frontend-0.1-SNAPSHOT.jar ec2-user@$TARGET_HOST:/home/ec2-user/build/fe
scp  -i $KEY  ${IMPALA_HOME}/shell/*  ec2-user@$TARGET_HOST:/home/ec2-user/build/shell/

for jar in `ls ${IMPALA_HOME}/fe/target/dependency/*.jar`; do
 scp  -i $KEY $jar ec2-user@$TARGET_HOST:/home/ec2-user/build/fe/dependency/
done




scp  -i $KEY /usr/lib/x86_64-linux-gnu/libboost_thread.so.1.54.0  ec2-user@$TARGET_HOST:/home/ec2-user/build/lib

scp  -i $KEY /usr/lib/x86_64-linux-gnu/libboost_regex.so.1.54.0  ec2-user@$TARGET_HOST:/home/ec2-user/build/lib

scp  -i $KEY /usr/lib/x86_64-linux-gnu/libboost_date_time.so.1.54.0  ec2-user@$TARGET_HOST:/home/ec2-user/build/lib
scp  -i $KEY /usr/lib/x86_64-linux-gnu/libboost_filesystem.so.1.54.0  ec2-user@$TARGET_HOST:/home/ec2-user/build/lib
scp  -i $KEY /usr/lib/x86_64-linux-gnu/libboost_system.so.1.54.0  ec2-user@$TARGET_HOST:/home/ec2-user/build/lib
scp  -i $KEY /usr/lib/x86_64-linux-gnu/libicuuc.so.52.1  ec2-user@$TARGET_HOST:/home/ec2-user/build/lib
scp  -i $KEY /usr/lib/x86_64-linux-gnu/libicui18n.so.52.1  ec2-user@$TARGET_HOST:/home/ec2-user/build/lib
scp  -i $KEY /usr/lib/x86_64-linux-gnu/libicudata.so.52.1  ec2-user@$TARGET_HOST:/home/ec2-user/build/lib

SCRIPT=$(readlink -f "$0")
SCRIPTPATH=$(dirname "$SCRIPT")

$SCRIPTPATH\deploy_scripts.sh
