CREATE TABLE dest1(c1 INT, c2 STRING) STORED AS TEXTFILE;

set mapred.job.tracker=does-not-exist:70;
set mapreduce.jobtracker.address=does-not-exist:70;

set hive.exec.mode.local.auto=true;
set hive.exec.mode.local.auto.tasks.max=7;

EXPLAIN
FROM src JOIN srcpart ON src.key = srcpart.key AND srcpart.ds = '2008-04-08' and src.key > 100
INSERT OVERWRITE TABLE dest1 SELECT src.key, srcpart.value;

FROM src JOIN srcpart ON src.key = srcpart.key AND srcpart.ds = '2008-04-08' and src.key > 100
INSERT OVERWRITE TABLE dest1 SELECT src.key, srcpart.value;

select dest1.* from dest1;
