#/etc/init.d/hive-metastore stop
sudo /etc/init.d/impala-server stop
sudo /etc/init.d/impala-catalog stop
sudo /etc/init.d/impala-state-store stop

#/etc/init.d/hive-metastore start
sudo /etc/init.d/impala-state-store start
sudo /etc/init.d/impala-catalog start
sudo /etc/init.d/impala-server start

