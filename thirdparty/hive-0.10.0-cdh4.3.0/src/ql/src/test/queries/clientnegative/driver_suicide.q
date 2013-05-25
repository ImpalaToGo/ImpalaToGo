CREATE TABLE SAMPLETABLE(IP STRING , showtime BIGINT ) partitioned by (ds string,ipz int) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\040';

ALTER TABLE SAMPLETABLE add Partition(ds='sf') location '/user/hive/warehouse' Partition(ipz=100) location '/user/hive/warehouse';

drop table SAMPLETABLE;