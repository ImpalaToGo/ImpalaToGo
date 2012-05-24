set mapreduce.framework.name=yarn;
set mapred.job.tracker=does-not-exist:70;
set mapreduce.jobtracker.address=does-not-exist:70;
set hive.exec.mode.local.auto.inputbytes.max=1;
set hive.exec.mode.local.auto=true;

SELECT key FROM src; 
