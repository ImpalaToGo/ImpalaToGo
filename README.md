ImpalaToGo
==========

A standalone distribution of Impala, optimized to work with cloud storage, bypassing HDFS completely.

### Why ImpalaToGo
----
>1. Remove the hard dependency to hdfs for those who want impala running for other distributed filesystems.
2. Run fast queries on your machine without any extra setup. Hive metastore service is enough prerequisite to start work with impala, no extra installations/configurations required.
3. No dependency on hdfs, thus, data locality is emulated via new layer, the cache, and is achieved via adaptive algorithms to map remote dfs data to impala cluster nodes cache.
4. Transparent interaction with user via new set of commands. All operations are user-driven and user-friendly. 
5. All shell commands along with impala cluster statistics are available via web interface.
6. Potential integration with any distributed file system via the compatible plugin.
7. ImpalaToGo has a lot of ideas so far to make impala usage simple!
 


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

License
----

[Apache License](http://www.apache.org/licenses/LICENSE-2.0.htm)



####2014

