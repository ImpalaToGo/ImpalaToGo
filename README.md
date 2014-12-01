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
    
  - **configure boost.** Default recent dev boost package is not compatible with impala. Download boost 1.46.1 and build packages required by impala (always can be found in ${IMPALA_HOME}/CMakeLists.txt):
    ####thread regex-mt system-mt filesystem-mt date_time

    1. **First fix some bugs in boost sources.**
    	* Being built with gcc, boost thread package encounters the conflict between gcc’s TIME\_UTC  (defined in 		/usr/include/time.h) and      self-defined TIME\_UTC (defined in  “boost path”/boost/thread/xtime.hpp). 	This happens because xtime.hpp includes “boost path”/boost/thread/thread\_time.hpp which in turn reference 		time.h from gcc’s headers.
	There’s a bug on this that existed for a long time cross release
	https://svn.boost.org/trac/boost/ticket/6940
	and was fixed only in latest boost release.
	To fix this, all TIME\_UTC menions in boost sources should be changed to TIME\_UTC\_ to get the rid of 			conflict with gcc.
	**See changelist:** https://svn.boost.org/trac/boost/changeset/78973#file3
	To fast apply the fix,
	in boost sources tree:
	```sh
	find . -type f -print0 | xargs -0 sed -i 's/TIME_UTC/TIME_UTC_/g'
	```
	
	* If compiled with gcc/g++ 4.7 and higher, where the reference to pthreads have changed to 				GLIBCXX_HAS_GTHREADS, so boost is unable to find pthreads and disable it.
     	Bug: https://svn.boost.org/trac/boost/ticket/6165
	Thus need to patch the file "your boost folder"/boost/config/stdlib/libstdcpp3.hpp
	Required change is described here: 					
	https://svn.boost.org/trac/boost/attachment/ticket/6165/libstdcpp3.hpp.patch
	In short, in file following should be changed:

	```c
	#ifdef __GLIBCXX__ // gcc 3.4 and greater: 
	# if defined(_GLIBCXX_HAVE_GTHR_DEFAULT) \ 
	|| defined(_GLIBCXX__PTHREADS) 
	// If the std lib has thread support turned on, then turn it on in 
	// Boost as well. We do this because some gcc-3.4 std lib headers
	// define _REENTANT while others do not... 
	// 
	# define BOOST_HAS_THREADS 
	# else 
	# define BOOST_DISABLE_THREADS 
	# endif
	```
	
	To
	
	```c
	#ifdef __GLIBCXX__ // gcc 3.4 and greater: 
	# if defined(_GLIBCXX_HAVE_GTHR_DEFAULT) \ 
	|| defined(_GLIBCXX__PTHREADS) \ 
	|| defined(_GLIBCXX_HAS_GTHREADS) 
	// gcc 4.7 
	// If the std lib has thread support turned on, then turn it on in Boost 
	// as well. We do this because some gcc-3.4 std lib headers define _REENTANT 
	// while others do not... 
	// 
	# define BOOST_HAS_THREADS 
	# else 
	# define BOOST_DISABLE_THREADS 
	# endif
	```
	
	- Fix in boost/cstdint.hpp:
    	
	```c
	// typedef  ::boost::long_long_type            int64_t;
	typedef long int int64_t;
	// typedef  ::boost::ulong_long_type   uint64_t;
	typedef long unsigned int uint64_t;
	```
	
	This will resolve ambiguous definitions (between system sys/types.h and boost’s boost/cstdint.hpp)
	And another fix here (put new lines instead of commented lines)
	
	```c
	// #if defined(BOOST_HAS_STDINT_H) && (!defined(__GLIBC__) ||
	// defined(__GLIBC_HAVE_LONG_LONG))
	# if defined(BOOST_HAS_STDINT_H)					\
  	&& (!defined(__GLIBC__)					\
      	|| defined(__GLIBC_HAVE_LONG_LONG)			\
      	|| (defined(__GLIBC__) && ((__GLIBC__ > 2) || ((__GLIBC__ == 2) && 
  	(__GLIBC_MINOR__ >= 17)))))
	```
        
 	- Fix for boost::shared_ptr (copy constructor is missed). See   https://svn.boost.org/trac/boost/changeset/73202.
        Fast fix: in shared_ptr.hpp ( /usr/local/include/boost/smart_ptr/shared_ptr.hpp) add default copy constructor (c++11 only)

       ```c 
       shared_ptr(const shared_ptr&) = default;
       ```
        
    2. **Fast reference to build the boost.**
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
put the following lines in Your .bashrc (replace YOUR_PATH with path where you cloned ImpalaToGo sources
```sh
export JAVA_HOME=/usr/lib/jvm/java-7-oracle
export IMPALA_HOME=YOUR_PATH/ImpalaToGo
export BOOST_LIBRARYDIR=/usr/lib64
export LD_LIBRARY_PATH=/usr/lib64
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
Before to start any impala service(impalad, catalogd, statestored) run prerequisites in shell (for environment variables setup on the shell). Here /usr/lib64 is the boost libraries location, specify yours there:

```sh
export JAVA_HOME=/usr/lib/jvm/java-7-oracle
export IMPALA_HOME=/home/elenav/src/ImpalaToGo
export BOOST_LIBRARYDIR=/usr/lib64
 . bin/impala-config.sh
export LD_LIBRARY_PATH=/usr/lib64
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

