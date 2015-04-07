#cdh5 target
TARGET_HOST=ec2-54-146-230-1.compute-1.amazonaws.com

#authorization 
KEY=/root/key/ImpalaToGo.pem
BASE_DIR=/root/ImpalaToGo/


#tachyon deployment 
ssh  -i $KEY ec2-user@$TARGET_HOST 'mkdir -p /home/ec2-user/build/tachyon'
ssh  -i $KEY ec2-user@$TARGET_HOST 'mkdir -p /home/ec2-user/build/tachyon/core/target/'
ssh  -i $KEY ec2-user@$TARGET_HOST 'mkdir -p /home/ec2-user/build/tachyon/core/src/main/webapp/'
ssh  -i $KEY ec2-user@$TARGET_HOST 'mkdir -p /home/ec2-user/build/tachyon/client/target/'
ssh  -i $KEY ec2-user@$TARGET_HOST 'mkdir -p /home/ec2-user/build/tachyon/assembly/target/'
ssh  -i $KEY ec2-user@$TARGET_HOST 'mkdir -p /home/ec2-user/build/tachyon/conf/'
ssh  -i $KEY ec2-user@$TARGET_HOST 'mkdir -p /home/ec2-user/build/tachyon/bin/'
ssh  -i $KEY ec2-user@$TARGET_HOST 'mkdir -p /home/ec2-user/build/tachyon/libexec/'

scp -rp -i $KEY BASE_DIR/thirdparty/tachyon-0.7.0-SNAPSHOT/core/target/*.jar ec2-user@$TARGET_HOST:/home/ec2-user/build/tachyon/core/target/
scp -rp -i $KEY BASE_DIR/thirdparty/tachyon-0.7.0-SNAPSHOT/core/src/main/webapp/* ec2-user@$TARGET_HOST:/home/ec2-user/build/tachyon/core/src/main/webapp/
scp -rp -i $KEY BASE_DIR/thirdparty/tachyon-0.7.0-SNAPSHOT/client/target/*.jar ec2-user@$TARGET_HOST:/home/ec2-user/build/tachyon/client/target/
scp -rp -i $KEY BASE_DIR/thirdparty/tachyon-0.7.0-SNAPSHOT/assembly/target/*.jar ec2-user@$TARGET_HOST:/home/ec2-user/build/tachyon/assembly/target/

scp -rp -i $KEY BASE_DIR/thirdparty/tachyon-0.7.0-SNAPSHOT/conf/* ec2-user@$TARGET_HOST:/home/ec2-user/build/tachyon/conf/
scp -rp -i $KEY BASE_DIR/thirdparty/tachyon-0.7.0-SNAPSHOT/bin/* ec2-user@$TARGET_HOST:/home/ec2-user/build/tachyon/bin/
scp -rp -i $KEY BASE_DIR/thirdparty/tachyon-0.7.0-SNAPSHOT/libexec/* ec2-user@$TARGET_HOST:/home/ec2-user/build/tachyon/libexec/

#impala deployment
scp -rp -i $KEY BASE_DIR/llvm-ir/* ec2-user@$TARGET_HOST:/home/ec2-user/build/llvm-ir/

scp -rp -i $KEY BASE_DIR/be/build/debug/catalog/catalogd ec2-user@$TARGET_HOST:/home/ec2-user/build/be/
scp -rp -i $KEY BASE_DIR/be/build/debug/statestore/statestored   ec2-user@$TARGET_HOST:/home/ec2-user/build/be/

#thirdparty deployment
scp -rp -i $KEY BASE_DIR/thirdparty/thrift-0.9.0/build/*  ec2-user@$TARGET_HOST:/home/ec2-user/build/thirdparty/thrift/build/

scp  -i $KEY BASE_DIR/bin/set-pythonpath.sh ec2-user@$TARGET_HOST:/home/ec2-user/build/bin/

#shell deployment
scp -rp  -i $KEY BASE_DIR/shell/* ec2-user@$TARGET_HOST:/home/ec2-user/build/shell/

#backend
scp  -i $KEY BASE_DIR/be/build/debug/service/* ec2-user@$TARGET_HOST:/home/ec2-user/build/be

#webap
scp -i $KEY BASE_DIR/www/* ec2-user@$TARGET_HOST:/home/ec2-user/build/www

#frontend
scp  -i $KEY BASE_DIR/fe/target/impala-frontend-0.1-SNAPSHOT.jar ec2-user@$TARGET_HOST:/home/ec2-user/build/fe
scp  -i $KEY  ${IMPALA_HOME}/shell/*  ec2-user@$TARGET_HOST:/home/ec2-user/build/shell/

#jars dependencies deployment
for jar in `ls ${IMPALA_HOME}/fe/target/dependency/*.jar`; do
 scp  -i $KEY $jar ec2-user@$TARGET_HOST:/home/ec2-user/build/fe/dependency/
done

#boost deployment
scp  -i $KEY /usr/lib/x86_64-linux-gnu/libboost_thread.so.1.54.0  ec2-user@$TARGET_HOST:/home/ec2-user/build/lib
scp  -i $KEY /usr/lib/x86_64-linux-gnu/libboost_regex.so.1.54.0  ec2-user@$TARGET_HOST:/home/ec2-user/build/lib
scp  -i $KEY /usr/lib/x86_64-linux-gnu/libboost_date_time.so.1.54.0  ec2-user@$TARGET_HOST:/home/ec2-user/build/lib
scp  -i $KEY /usr/lib/x86_64-linux-gnu/libboost_filesystem.so.1.54.0  ec2-user@$TARGET_HOST:/home/ec2-user/build/lib
scp  -i $KEY /usr/lib/x86_64-linux-gnu/libboost_system.so.1.54.0  ec2-user@$TARGET_HOST:/home/ec2-user/build/lib

#libicu deployment
scp  -i $KEY /usr/lib/x86_64-linux-gnu/libicuuc.so.52.1  ec2-user@$TARGET_HOST:/home/ec2-user/build/lib
scp  -i $KEY /usr/lib/x86_64-linux-gnu/libicui18n.so.52.1  ec2-user@$TARGET_HOST:/home/ec2-user/build/lib
scp  -i $KEY /usr/lib/x86_64-linux-gnu/libicudata.so.52.1  ec2-user@$TARGET_HOST:/home/ec2-user/build/lib

#deployment section
scp  -i $KEY BASE_DIR/deployment/install.sh ec2-user@$TARGET_HOST:/home/ec2-user/build/