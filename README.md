ImpalaToGo
==========

Fork of Cloudera Impala, separated from hadoop. It is optimized to work with S3 storage by caching data locally.

### Why ImpalaToGo
----
>1. It is Impala without hadoop. You can take advantage of its fast query engine without managing whole hadoop stack.
2. Optimized work with S3. ImpalaToGo transparently cache data on local drives.
3. Actually only open source MPP database written in C++
4. It gives you almost the same capabilities as Hive over S3, but much faster.

 
How to try: https://github.com/ImpalaToGo/ImpalaToGo/wiki/How-to-try

Short presentation on ImpalaToGo architecture: http://www.slideshare.net/DavidGroozman/impala-togo-introduction

###  What we add to Cloudera Impala?
We are developing caching layer which use local drives to cache access to remote storage. Its advantage can be easy to be seen when working with S3.

Development environment prerequisites (Ubuntu)
----

  - Install Java
    ```sh
    sudo apt-get install python-software-properties
    sudo add-apt-repository ppa:webupd8team/java -y
    sudo apt-get update -y
    sudo apt-get install oracle-jdk7-installer -y
    ```
    
  - install other prerequsites
      ```sh
    sudo apt-get install git build-essential cmake bison flex pkg-config libsasl2-dev autoconf automake libtool maven subversion doxygen libbz2-dev zlib1g-dev  python-setuptools python-dev libssl-dev -y
    ```
  - install recent gcc and g++ (4.9 for now)
   ```sh
    sudo apt-get update
    sudo apt-get install gcc-4.9
    sudo apt-get install g++-4.9
    sudo update-alternatives --install /usr/bin/gcc gcc /usr/bin/gcc-4.9 20
    sudo update-alternatives --install /usr/bin/g++ g++ /usr/bin/g++-4.9 20
    sudo update-alternatives --config gcc
    sudo update-alternatives --config g++
    ```
    
  - **configure boost in 1 click.** Now, default boost package is compatible with Impala, thus in order to configure boost, regular dev package is enough ( note that boost version should be >= 1.46.1 )
```sh
sudo apt-get install libboost-all-dev
```
Just be careful to have environment clear from boost artifacts before deploying new boost package (especially using apt-get).
To get rid of old possible boost artifacts:
```sh
sudo apt-get --purge remove libboost-dev
sudo apt-get --purge remove libboost-all-dev

# check installed boost version
cat /usr/include/boost/version.hpp | grep "BOOST_LIB_VERSION"

# check Boost package name:
dpkg -S /usr/include/boost/version.hpp

# Then having package name:
sudo apt-get autoremove package

sudo apt-get remove libboost*
sudo apt-get autoclean
```

  - **Fast reference to build the boost manually.**
    Newest boost builds do not contain packages with -mt prefixes as stated by boost.
    
    > To build libraries with -mt sufix:
     
    ```sh
    ./bootstrap.sh --with-libraries=filesystem, regex, system
    sudo ./bjam --layout=tagged --libdir=/usr/lib64 cxxflags=-fPIC threading=multi install
    ```
     > To build libraries without a suffix:  

    ```sh
    ./bootstrap.sh --with-libraries=thread, date_time, system
    sudo ./bjam --libdir=/usr/lib64 cxxflags=-fPIC link=static threading=single runtime-link=static install
    ```
   
   
- install hive metastore database, for example, postgres, if going to run it locally

```sh
sudo apt-get install postgresql -y
```

- compile LLVM from source
```sh
wget http://llvm.org/releases/3.3/llvm-3.3.src.tar.gz
tar xvf llvm-3.3.src.tar.gz
cd llvm-3.3.src/tools/
svn co http://llvm.org/svn/llvm-project/cfe/tags/RELEASE_33/final/ clang
cd ../projects/
svn co http://llvm.org/svn/llvm-project/compiler-rt/tags/RELEASE_33/final
cd ..
./configure --with-pic
make -j4 REQUIRES_RTTI=1
sudo make install
```

- configure JAVA HOME
```sh
export JAVA_HOME=/usr/lib/jvm/java-7-oracle
```

- install Maven
```sh
wget http://www.interior-dsgn.com/apache/maven/maven-3/3.0.5/binaries/apache-maven-3.0.5-bin.tar.gz
tar xvf apache-maven-3.0.5-bin.tar.gz && sudo mv apache-maven-3.0.5 /usr/local
```

- Add the following three lines to your .bashrc:
```sh
export M2_HOME=/usr/local/apache-maven-3.0.5
export M2=$M2_HOME/bin  
export PATH=$M2:$PATH
```
Confirm by running
```sh
source ~/.bashrc
mvn -version
```

Impala build
----
put the following lines in Your .bashrc (replace YOUR_PATH with path where you cloned ImpalaToGo sources.  BOOST_LIBRARY_DIR and LD_LIBRARY_DIR should point to boost libraries location
```sh
export JAVA_HOME=/usr/lib/jvm/java-7-oracle
export IMPALA_HOME=YOUR_PATH/ImpalaToGo
export BOOST_LIBRARYDIR=/usr/lib/x86_64-linux-gnu
export LD_LIBRARY_PATH=/usr/lib/x86_64-linux-gnu
export LC_ALL="en_US.UTF-8"
```
and then run
```sh
source ~/.bashrc
```

- cd to Impala source cloned dir, run 
```sh
bin/impala-config.sh
```

- build thirdparty :
```sh
./bin/build_thirdparty.sh
```

- run impala build:
```sh
./buildall.sh
```

To run unit testing on backend , do the following in the ImpalaToGo bin directory
```sh
export IMPALA_BE_DIR=/root/ImpalaToGo/be/
./run-backend-tests.sh

```

Start work with impala
----

To run Impala locally, edit Impala configuration files.
Impala will read configuration files that it founds on CLASSPATH. By defaut, fe test resources are added on CLASSPATH already ($IMPALA_HOME/fe/src/test/resources), so core-site.xml, hive-site.xml and hdfs-site.xml can be placed and edited there.
Note that they will be rewritten with default values after frontend build.

1. ####Make changes in configuration files to run impala locally.
    > In core-site.xml, following properties should be set to file:///
    ```xml
  <property>
    <name>fs.defaultFS</name>
    <value>file:///</value>
  </property>
  <property>
    <name>fs.default.name</name>
    <value>file:///</value>
    <description>The name of the default file system.  A URI whose scheme and authority determine the FileSystem implementation.</description>
  </property>
```
  
 > In hive-site.xml, specify connection string, hive user and a password for one (for metastore usage). 
    
2. ####Start all services.
Before to start any impala service(impalad, catalogd, statestored) run prerequisites in shell (for environment variables setup on the shell). Here /usr/lib/x86_64-linux-gnu is the boost libraries location, check and specify yours:

```sh
export JAVA_HOME=/usr/lib/jvm/java-7-oracle
export IMPALA_HOME=/home/elenav/src/ImpalaToGo
export BOOST_LIBRARYDIR=/usr/lib/x86_64-linux-gnu
 . bin/impala-config.sh
export LD_LIBRARY_PATH=/usr/lib/x86_64-linux-gnu
```
- **start metastore:**
For the first time, run the script:

```sh
${IMPALA_HOME}/bin/create-test-configuration.sh
```
this will create the hive_impalatogo database and the hiveuser user in postgresql along with all required 		permissions.
Note this is needed only for the first time and is usually done by buildall script.

start metastore:

```sh
cd thirdparty/hive-0.10.0-cdh4.5.0/
bin/hive --service metastore
```
- **start statestored:**
```sh
${IMPALA_HOME}/bin/start-statestore.sh
```
- **start catalogd:**
```sh
${IMPALA_HOME}/bin/start-catalogd.sh
```
- **start impalad:**
```sh
${IMPALA_HOME}/bin/start-impalad.sh
```
####Test impala with the local file via impala shell:

- **start impala shell:**

```sh
  . bin/impala-shell.sh
```

When running on the same data node as Impalad, no need to connect explcitly. The prompt is automatically shows the conection to Impalad:

[impalad_hostname:21000] >

Place a csv file to some local directory and run some statements on this location to be sure impala works for you:

```sql
create external table test_table (name string, category string, score double) ROW FORMAT DELIMITED fields terminated BY '\t' lines terminated BY '\n' location '/home/username/src/ImpalaToGo/datastorage/';
```

```sql
select * from test_table where category="xyz";
```

To run Impala on Tachyon
----

Impala should integrate with Tachyon as with a new filesystem (tachyon)

1. ####Make changes in configuration files.
    > In core-site.xml, following properties should be set to tachyon://...
   ```xml
     <property>
       <name>fs.defaultFS</name>
       <value>tachyon://localhost:19998</value>
     </property>
     <property>
       <name>fs.default.name</name>
       <value>tachyon://localhost:19998</value>
       <description>The name of the default file system.  A URI whose scheme and authority determine the  FileSystem implementation.</description>
     </property>
     <property>
       <name>fs.tachyon.impl</name>
       <value>tachyon.hadoop.TFS</value>
     </property>
   ```

2. ####Edit tachyon/conf/tachyon-env.sh
    > change **TACHYON_UNDERFS_ADDRESS** - set to the underlying dfs address. 
   For s3, add key id and secret key to **TACHYON_JAVA_OPTS** :

   ```bash
     -Dfs.s3n.awsAccessKeyId=123
     -Dfs.s3n.awsSecretAccessKey=456
   ```   
   Example of tachyon-env.sh:

   ```bash
   !/usr/bin/env bash
   
   # This file contains environment variables required to run Tachyon. Copy it as tachyon-env.sh and
   # edit that to configure Tachyon for your site. At a minimum,
   # the following variables should be set:
   #
   # - JAVA_HOME, to point to your JAVA installation
   # - TACHYON_MASTER_ADDRESS, to bind the master to a different IP address or hostname
   # - TACHYON_UNDERFS_ADDRESS, to set the under filesystem address.
   # - TACHYON_WORKER_MEMORY_SIZE, to set how much memory to use (e.g. 1000mb, 2gb) per worker
   # - TACHYON_RAM_FOLDER, to set where worker stores in memory data
   # - TACHYON_UNDERFS_HDFS_IMPL, to set which HDFS implementation to use (e.g. com.mapr.fs.MapRFileSystem,
   #   org.apache.hadoop.hdfs.DistributedFileSystem)
   
   # The following gives an example:
   
   if [[ `uname -a` == Darwin* ]]; then
     # Assuming Mac OS X
     export JAVA_HOME=${JAVA_HOME:-$(/usr/libexec/java_home)}
     export TACHYON_RAM_FOLDER=/Volumes/ramdisk
     export TACHYON_JAVA_OPTS="-Djava.security.krb5.realm= -Djava.security.krb5.kdc="
   else
     # Assuming Linux
     if [ -z "$JAVA_HOME" ]; then
       export JAVA_HOME=/usr/lib/jvm/java-7-openjdk-amd64
     fi
     export TACHYON_RAM_FOLDER=/mnt/ramdisk
   fi
   
   export JAVA="$JAVA_HOME/bin/java"
   export TACHYON_MASTER_ADDRESS=localhost
   export TACHYON_UNDERFS_ADDRESS=s3n://amazon_bucket
   # for hdfs, uncomment this --> export TACHYON_UNDERFS_ADDRESS=hdfs://localhost:9000
   export TACHYON_WORKER_MEMORY_SIZE=0.5GB
   export TACHYON_UNDERFS_HDFS_IMPL=org.apache.hadoop.hdfs.DistributedFileSystem
   
   CONF_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
   
   export TACHYON_JAVA_OPTS+="
     -Dlog4j.configuration=file:$CONF_DIR/log4j.properties
     -Dtachyon.debug=false
     -Dtachyon.underfs.address=$TACHYON_UNDERFS_ADDRESS
     -Dtachyon.underfs.hdfs.impl=$TACHYON_UNDERFS_HDFS_IMPL
     -Dtachyon.data.folder=$TACHYON_UNDERFS_ADDRESS/tmp/tachyon/data
     -Dtachyon.workers.folder=$TACHYON_UNDERFS_ADDRESS/tmp/tachyon/workers
     -Dtachyon.worker.memory.size=$TACHYON_WORKER_MEMORY_SIZE
     -Dtachyon.worker.data.folder=$TACHYON_RAM_FOLDER/tachyonworker/
     -Dtachyon.master.worker.timeout.ms=60000
     -Dtachyon.master.hostname=$TACHYON_MASTER_ADDRESS
     -Dtachyon.master.journal.folder=$TACHYON_HOME/journal/
     -Dorg.apache.jasper.compiler.disablejsr199=true
     -Djava.net.preferIPv4Stack=true
     -Dfs.s3n.awsAccessKeyId=123
     -Dfs.s3n.awsSecretAccessKey=456
   "
   
   # Master specific parameters. Default to TACHYON_JAVA_OPTS.
   export TACHYON_MASTER_JAVA_OPTS="$TACHYON_JAVA_OPTS"
   
   # Worker specific parameters that will be shared to all workers. Default to TACHYON_JAVA_OPTS.
   export TACHYON_WORKER_JAVA_OPTS="$TACHYON_JAVA_OPTS"
   ```

3. ####For s3, an extra dependecnies are required.
    > The **hadoop-client** package requires the jets3t package to use S3, but for some reason doesn't pull it in    as a depedency.One way to fix this is to repackage Tachyon, adding jets3t as a dependency. For example, the    following should work with hadoop version 2.5.0, although depending on your version of Hadoop, you may need an  older version of jets3t:
   ```xml
   <dependency>
     <groupId>net.java.dev.jets3t</groupId>
     <artifactId>jets3t</artifactId>
     <version>0.9.0</version>
     <exclusions>
       <exclusion>
         <groupId>commons-codec</groupId>
         <artifactId>commons-codec</artifactId>
         <!-- <version>1.3</version> -->
       </exclusion>
     </exclusions>
   </dependency>
   ```

    > For tachyon 0.6.0 to work with hadoop 2.5.0, you'll have to export first paths to : 
   - * jets3:0.9.0 
   - * commons-httpclient:3.1

   ```bash
   export TACHYON_CLASSPATH=~/.m2/repository/commons-httpclient/commons-httpclient/3.1/commons-httpclient-3.1.jar:~/.m2/repository/net/java/dev/jets3t/jets3t/0.9.0/jets3t-0.9.0.jar
   ```

    > Note that Impala thirdparty contains these dependencies in hadoop snapshot: 
   ```bash
   ${IMPALA_HOME}/thirdparty/cdh5.2.0/hadoop-2.5.0-cdh5.2.0/share/hadoop/tools/lib
   ```

4. ####Run Tachyon:
   ```bash
   $ cd tachyon
   $ ./bin/format.sh
   $ ./bin/start.sh all
   ```

5. ####Export tachyon client location to Impala. 
    > For production, edit /etc/bin/impalad and add the path to tachyon client there
   ```bash
   export CLASSPATH=/pathToTachyon/client/target/tachyon-client-{{site.TACHYON_RELEASED_VERSION}}-jar-with-dependencies.jar:$CLASSPATH
   ```

6. ####Run ImpalaToGo

License
----

[Apache License](http://www.apache.org/licenses/LICENSE-2.0.htm)



####2014-2015

