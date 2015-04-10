#cdh5 target
#authorization 
if [  -d "$IMPALA_HOME" ];
then
	BASE_DIR=$IMPALA_HOME
else
	BASE_DIR=/root/ImpalaToGo/
fi
PACKAGE_DIR=$BASE_DIR/package
REMOTE_BASE_DIR=$PACKAGE_DIR/build
#tachyon deployment
TACHYON_DIR=tachyon-0.7.0-SNAPSHOT
mkdir -p $REMOTE_BASE_DIR/tachyon/
mkdir -p $REMOTE_BASE_DIR/tachyon/core/target/
mkdir -p $REMOTE_BASE_DIR/tachyon/core/src/main/webapp/
mkdir -p $REMOTE_BASE_DIR/tachyon/client/target/
mkdir -p $REMOTE_BASE_DIR/tachyon/assembly/target/
mkdir -p $REMOTE_BASE_DIR/tachyon/conf/
mkdir -p $REMOTE_BASE_DIR/tachyon/bin/
mkdir -p $REMOTE_BASE_DIR/tachyon/libexec/


mkdir -p $REMOTE_BASE_DIR/llvm-ir/
mkdir -p $REMOTE_BASE_DIR/be/
mkdir -p $REMOTE_BASE_DIR/thirdparty/thrift/build/
mkdir -p $REMOTE_BASE_DIR/bin/
mkdir -p $REMOTE_BASE_DIR/shell/
mkdir -p $REMOTE_BASE_DIR/lib/


cp -rpv $BASE_DIR/thirdparty/$TACHYON_DIR/core/target/*.jar $REMOTE_BASE_DIR/tachyon/core/target/
cp -rpv $BASE_DIR/thirdparty/$TACHYON_DIR/core/src/main/webapp/* $REMOTE_BASE_DIR/tachyon/core/src/main/webapp/
cp -rpv $BASE_DIR/thirdparty/$TACHYON_DIR/client/target/*.jar $REMOTE_BASE_DIR/tachyon/client/target/
cp -rpv $BASE_DIR/thirdparty/$TACHYON_DIR/assembly/target/*.jar $REMOTE_BASE_DIR/tachyon/assembly/target/

cp -rpv $BASE_DIR/thirdparty/$TACHYON_DIR/conf/* $REMOTE_BASE_DIR/tachyon/conf/
cp -rpv $BASE_DIR/thirdparty/$TACHYON_DIR/bin/* $REMOTE_BASE_DIR/tachyon/bin/
cp -rpv $BASE_DIR/thirdparty/$TACHYON_DIR/libexec/* $REMOTE_BASE_DIR/tachyon/libexec/

#impala deployment
cp -rpv $BASE_DIR/llvm-ir/* $REMOTE_BASE_DIR/llvm-ir/

cp -rpv $BASE_DIR/be/build/debug/catalog/catalogd $REMOTE_BASE_DIR/be/
cp -rpv $BASE_DIR/be/build/debug/statestore/statestored   $REMOTE_BASE_DIR/be/

#thirdparty deployment
cp -rpv $BASE_DIR/thirdparty/thrift-0.9.0/build/*  $REMOTE_BASE_DIR/thirdparty/thrift/build/
cp -rpv $BASE_DIR/bin/set-pythonpath.sh $REMOTE_BASE_DIR/bin/

#shell deployment
cp -rpv $BASE_DIR/shell/* $REMOTE_BASE_DIR/shell/
cp -pv $BASE_DIR/bin/impala-shell.sh $REMOTE_BASE_DIR/bin/
cp -pv $BASE_DIR/deployment/cluster/control/shell.sh $REMOTE_BASE_DIR/bin/

#backend
cp -rpv $BASE_DIR/be/build/debug/service/* $REMOTE_BASE_DIR/be

#webap
mkdir -p $REMOTE_BASE_DIR/www/
cp -rpv $BASE_DIR/www/* $REMOTE_BASE_DIR/www

#frontend
mkdir -p $REMOTE_BASE_DIR/fe/
cp -rpv $BASE_DIR/fe/target/impala-frontend-0.1-SNAPSHOT.jar $REMOTE_BASE_DIR/fe
cp -rpv  ${IMPALA_HOME}/shell/*  $REMOTE_BASE_DIR/shell/

#jars dependencies deployment
mkdir -p $REMOTE_BASE_DIR/fe/dependency/
for jar in $(ls ${IMPALA_HOME}/fe/target/dependency/*.jar); do
 cp -rpv $jar $REMOTE_BASE_DIR/fe/dependency/
done

#boost deployment
cp -rpv /usr/lib/x86_64-linux-gnu/libboost_thread.so.1.54.0  $REMOTE_BASE_DIR/lib
cp -rpv /usr/lib/x86_64-linux-gnu/libboost_regex.so.1.54.0  $REMOTE_BASE_DIR/lib
cp -rpv /usr/lib/x86_64-linux-gnu/libboost_date_time.so.1.54.0  $REMOTE_BASE_DIR/lib
cp -rpv /usr/lib/x86_64-linux-gnu/libboost_filesystem.so.1.54.0  $REMOTE_BASE_DIR/lib
cp -rpv /usr/lib/x86_64-linux-gnu/libboost_system.so.1.54.0  $REMOTE_BASE_DIR/lib

#libicu deployment
cp -rpv /usr/lib/x86_64-linux-gnu/libicuuc.so.52.1  $REMOTE_BASE_DIR/lib
cp -rpv /usr/lib/x86_64-linux-gnu/libicui18n.so.52.1  $REMOTE_BASE_DIR/lib
cp -rpv /usr/lib/x86_64-linux-gnu/libicudata.so.52.1  $REMOTE_BASE_DIR/lib

#deployment section
cp -rpv $BASE_DIR/deployment/install.sh $REMOTE_BASE_DIR/
echo -n Creating archive
tar -czf build.tgz -C $PACKAGE_DIR build
echo ...done
