# Cloudera Impala

Cloudera Impala is a distributed query execution engine that runs against data stored natively in Apache HDFS and Apache HBase. This public repository is a snapshot of our internal development repository that will be updated periodically as we prepare new releases. 

The rest of this README describes how to build Cloudera Impala from this repository. Further documentation about Cloudera Impala can be found [here](https://ccp.cloudera.com/display/IMPALA10BETADOC/Cloudera+Impala+1.0+Beta+Documentation). 

# Building Cloudera Impala on CentOS 6.2

## Prerequisites

### Installing prerequisite packages

    sudo yum install boost-test boost-program-options libevent-devel automake libtool flex bison gcc-c++ openssl-devel \
    make cmake doxygen.x86_64 glib-devel boost-devel python-devel bzip2-devel svn libevent-devel cyrus-sasl-devel \
    wget git unzip

*Note:* Ubuntu 12.04 (and later) requires the libevent1-dev package to work with Thrift v0.9 

### Install LLVM

    wget http://llvm.org/releases/3.2/llvm-3.2.src.tar.gz
    tar xvzf llvm-3.2.src.tar.gz
    cd llvm-3.2.src/tools
    svn co http://llvm.org/svn/llvm-project/cfe/tags/RELEASE_32/final/ clang
    cd ../projects
    svn co http://llvm.org/svn/llvm-project/compiler-rt/tags/RELEASE_32/final/ compiler-rt
    cd ..
    ./configure --with-pic
    make -j4 REQUIRES_RTTI=1
    sudo make install
    
### Install the JDK

Make sure that the Oracle Java Development Kit 6 is installed (not OpenJDK), and that `JAVA_HOME` is set in your environment.

### Install Maven

    wget http://www.fightrice.com/mirrors/apache/maven/maven-3/3.0.4/binaries/apache-maven-3.0.4-bin.tar.gz
    tar xvf apache-maven-3.0.4.tar.gz && sudo mv apache-maven-3.0.4 /usr/local
   
Add the following three lines to your .bashrc:

    export M2_HOME=/usr/local/apache-maven-3.0.4
    export M2=$M2_HOME/bin  
    export PATH=$M2:$PATH 

And make sure you pick up the changes either by logging in to a fresh shell or running:

    source ~/.bashrc

Confirm by running:

    mvn -version

and you should see at least:

    Apache Maven 3.0.4...

## Building Cloudera Impala

### Clone the Impala repository

    git clone https://github.com/cloudera/impala.git

### Set the Impala environment
  
    cd impala
    . bin/impala-config.sh

Confirm your environment looks correct:

    (11:11:21@desktop) ~/src/cloudera/impala-public (master) $ env | grep "IMPALA.*VERSION"
    IMPALA_CYRUS_SASL_VERSION=2.1.23
    IMPALA_HBASE_VERSION=0.92.1-cdh4.1.0
    IMPALA_SNAPPY_VERSION=1.0.5
    IMPALA_GTEST_VERSION=1.6.0
    IMPALA_GPERFTOOLS_VERSION=2.0
    IMPALA_GFLAGS_VERSION=2.0
    IMPALA_GLOG_VERSION=0.3.2
    IMPALA_HADOOP_VERSION=2.0.0-cdh4.1.0
    IMPALA_HIVE_VERSION=0.9.0-cdh4.1.0
    IMPALA_MONGOOSE_VERSION=3.3
    IMPALA_THRIFT_VERSION=0.9.0

### Download required third-party packages

    cd thirdparty
    ./download_thirdparty.sh

### Build Impala

    cd ${IMPALA_HOME}
    ./build_public.sh -build_thirdparty

## Wrapping up

After a successful build, there should be an `impalad` binary in `${IMPALA_HOME}/be/build/debug/service`.

You can start an Impala backend by running:

    ${IMPALA_HOME}/bin/start-impalad.sh -use_statestore=false

Note that the `start-impalad.sh` script sets some environment variables that are necessary for Impala to run successfully.

To configure Impala's use of HDFS, HBase or the Hive metastore, place the relevant configuration files somewhere in the `CLASSPATH` established by `bin/set-classpath.sh`. Internally we use `fe/src/test/resources` for this purpose, you may find it convenient to do the same.

## The Impala Shell

The Impala shell is a convenient command-line interface to Cloudera Impala. To run from a source repository, do the following:

    ${IMPALA_HOME}/bin/impala-shell.sh
