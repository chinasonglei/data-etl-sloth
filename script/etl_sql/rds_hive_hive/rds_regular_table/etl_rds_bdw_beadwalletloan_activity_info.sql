
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

    
INSERT OVERWRITE table rds.rds_bdw_beadwalletloan_activity_info
partition(is_valid,ce_start_date,ce_end_date)
SELECT * FROM
(
SELECT
A.`activity_id`,
A.`activity_title`,
A.`start_time`,
A.`end_time`,
A.`participant`,
A.`content`,
A.`activity_rule`,
A.`status`,
A.`limited_time`,
A.`valid_year`,
A.`valid_month`,
A.`valid_day`,
A.`limited_amount`,
A.`activity_type`,
A.`create_time`,
A.`activity_url`,
A.`activity_img`,
CASE WHEN B.activity_id IS NOT NULL THEN 0 ELSE A.is_valid
END AS is_valid,
A.ce_start_date,
CASE WHEN B.activity_id IS NOT NULL THEN B.dt
ELSE A.ce_end_date
END AS ce_end_date
FROM (select * from rds.rds_bdw_beadwalletloan_activity_info where is_valid=1) AS A
LEFT JOIN ods.ods_bdw_beadwalletloan_activity_info AS B
ON A.activity_id=B.activity_id and B.dt=${date}
union
SELECT
C.`activity_id`,
C.`activity_title`,
C.`start_time`,
C.`end_time`,
C.`participant`,
C.`content`,
C.`activity_rule`,
C.`status`,
C.`limited_time`,
C.`valid_year`,
C.`valid_month`,
C.`valid_day`,
CAST(C.limited_amount as decimal(38,6)),
C.`activity_type`,
C.`create_time`,
C.`activity_url`,
C.`activity_img`,
1 AS is_valid,
${date} AS ce_start_date,
99991231 AS ce_end_date
FROM ods.ods_bdw_beadwalletloan_activity_info AS C
where C.dt=${date}
)AS T;

insert into table rds.rds_bdw_beadwalletloan_activity_info 
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
null,
null,
null,
null,
null,
null,
1,
${date},
99991231;
