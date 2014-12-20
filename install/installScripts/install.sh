#install java and set java home
# is it ok?
sudo yum install python-devel openssl-devel python-pip
sudo yum install wget
 wget --no-cookies --no-check-certificate --header "Cookie: gpw_e24=http%3A%2F%2Fwww.oracle.com%2F; oraclelicense=accept-securebackup-cookie" "http://download.oracle.com/otn-pub/java/jdk/7u67-b01/jdk-7u67-linux-x64.tar.gz"

sudo mkdir /usr/java
sudo tar xzf jdk-7u67-linux-x64.tar.gz  -C /usr/java/
sudo ln -s /usr/java/jdk1.7.0_67 /usr/java/default
sudo alternatives --install /usr/bin/java java /usr/java/jdk1.7.0_67/bin/java 1

#install dependency

 sudo yum install redhat-lsb



#install hive

# create user
sudo useradd  --shell /bin/bash  --home /var/lib/impala  impala


sudo cp impalaScripts/impalad /usr/bin
sudo cp impalaScripts/statestored /usr/bin
sudo cp impalaScripts/catalogd /usr/bin


#install services
cd impalaScripts
sudo cp impala-server /etc/init.d/
sudo chkconfig --add impala-server

sudo cp impala-catalog /etc/init.d/
sudo chkconfig --add impala-catalog

sudo cp impala-state-store /etc/init.d/
sudo chkconfig --add impala-state-store

sudo cp hive-metastore /etc/init.d/
sudo chkconfig --add hive-metastore






#create directories

sudo mkdir -p /etc/hive/conf
sudo mkdir -p /etc/impala/conf

sudo mkdir /usr/lib/impala

sudo mkdir /usr/lib/impala/impalatogoLib
sudo mkdir /usr/lib/impala/llvm-ir
sudo mkdir /usr/lib/impala/thirdparty
sudo mkdir /usr/lib/impala/lib
sudo mkdir /usr/lib/impala/sbin
sudo mkdir /usr/lib/impala/www
sudo mkdir /usr/lib/impala/sbin-retail

sudo mkdir /var/log/impala/
sudo chown -R impala /var/log/impala/

sudo mkdir /var/cache/impalatogo/
sudo chown impala /var/cache/impalatogo/

sudo cp -f llvm-ir/*  /usr/lib/impala/llvm-ir/
sudo cp -f be/* /usr/lib/impala/sbin/
sudo rm -rf /usr/lib/impala/impalatogoLib/*
sudo cp fe/*.jar /usr/lib/impala/impalatogoLib/

sudo cp fe/dependency/* /usr/lib/impala/impalatogoLib/
sudo cp ~/build/lib/* /usr/lib/impala/lib/
sudo ln -s /usr/lib/impala/lib/libboost_thread.so.1.46.1 /usr/lib/impala/lib/libboost_thread.so
sudo ln -s /usr/lib/impala/lib/libboost_regex-mt.so.1.46.1 /usr/lib/impala/lib/libboost_regex-mt.so
sudo ln -s /usr/lib/impala/lib/libboost_date_time.so.1.46.1 /usr/lib/impala/lib/libboost_date_time.so
sudo ln -s /usr/lib/impala/lib/libboost_filesystem-mt.so.1.46.1  /usr/lib/impala/lib/libboost_filesystem-mt.so
sudo ln -s /usr/lib/impala/lib/libboost_system-mt.so.1.46.1 /usr/lib/impala/lib/libboost_system-mt.so
sudo ln -s /lib64/libbz2.so.1 /usr/lib/impala/lib/libbz2.so.1.0
sudo cp missinglib/* /usr/lib/impala/lib/
sudo chown -R impala  /usr/lib/impala/
sudo chown -R impala  /usr/lib/impala/llvm-ir

