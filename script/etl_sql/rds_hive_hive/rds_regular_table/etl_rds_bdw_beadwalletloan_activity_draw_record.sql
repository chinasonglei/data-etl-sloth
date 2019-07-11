
SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.exec.max.dynamic.partitions=200000;
SET hive.exec.max.dynamic.partitions.pernode=20000;
SET parquet.memory.min.chunk.size=100000;
SET hive.execution.engine=mr;
SET hive.mapjoin.smalltable.filesize=55000000;
SET hive.auto.convert.join=false;
set hive.optimize.sort.dynamic.partition = true;
set mapred.job.reuse.jvm.num.tasks=20;
set hive.exec.reducers.bytes.per.reducer=150000000;
set hive.exec.parallel=true;
set mapreduce.map.memory.mb=12288;
set mapreduce.reduce.memory.mb=8192;
set hive.exec.stagingdir=./temp;

    
INSERT OVERWRITE table rds.rds_bdw_beadwalletloan_activity_draw_record
partition(is_valid,ce_start_date,ce_end_date)
SELECT * FROM
(
SELECT
A.`id`,
A.`borrower_id`,
A.`activity_id`,
A.`is_winning`,
A.`prize_id`,
A.`grant_status`,
A.`contacts_name`,
A.`contacts_phone`,
A.`address`,
A.`create_time`,
A.`update_time`,
CASE WHEN B.id IS NOT NULL THEN 0 ELSE A.is_valid
END AS is_valid,
A.ce_start_date,
CASE WHEN B.id IS NOT NULL THEN B.dt
ELSE A.ce_end_date
END AS ce_end_date
FROM (select * from rds.rds_bdw_beadwalletloan_activity_draw_record where is_valid=1) AS A
LEFT JOIN ods.ods_bdw_beadwalletloan_activity_draw_record AS B
ON A.id=B.id and B.dt=${date}
union
SELECT
C.`id`,
C.`borrower_id`,
C.`activity_id`,
C.`is_winning`,
C.`prize_id`,
C.`grant_status`,
C.`contacts_name`,
C.`contacts_phone`,
C.`address`,
C.`create_time`,
C.`update_time`,
1 AS is_valid,
${date} AS ce_start_date,
99991231 AS ce_end_date
FROM ods.ods_bdw_beadwalletloan_activity_draw_record AS C
where C.dt=${date}
)AS T;

insert into table rds.rds_bdw_beadwalletloan_activity_draw_record 
partition(is_valid,ce_start_date,ce_end_date)
select
-2147483648,
null,
null,
null,
null,
null,
null,
null,
null,
null,
null,
1,
${date},
99991231;
