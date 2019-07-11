#!/bin/bash
CONNECTURL='jdbc:mysql://rr-uf6apuqe273f87c38xo.mysql.rds.aliyuncs.com:3306/beadwalletloan?tinyInt1isBit=false&characterEncoding=utf-8&autoReconnect=true&zeroDateTimeBehavior=convertToNull'
USERNAME=zhidubeadwallet
PASSWORD=zhidubeadwallet@2019
#table.conf
mysql_database=beadwalletloan
mysql_table=activity_discount_distribute
hive_external_table=ods_bdw_beadwalletloan_activity_discount_distribute

sqltemp=`cat /home/dispatch/Project/data-etl-sloth/schedule/etl_script/etl/ods_mysql_hive/ods_regular_table/etl_ods_bdw_beadwalletloan_activity_discount_distribute.sql`
v_dt=$1
end_dt=$2
#下面是基础查询语句 拼接 where条件
sql="$sqltemp where (a.create_time>="\"$v_dt\"" and a.create_time< "\"$end_dt\"") or (a.update_time>="\"$v_dt\"" and a.update_time< "\"$end_dt\"") and "\$CONDITIONS""
#分区指定为今日
 v_externalTablePartition=`date +%Y%m%d`
 

sqoop import --connect $CONNECTURL --username $USERNAME --password $PASSWORD --query "$sql" --target-dir "/user/hive/warehouse/ods/"$hive_external_table"/"$v_externalTablePartition --num-mappers 1 --as-parquetfile --driver com.mysql.jdbc.Driver 
hdfs dfs -mv /user/hive/warehouse/ods/$hive_external_table/$v_externalTablePartition /user/hive/warehouse/ods/$hive_external_table/dt=$v_externalTablePartition 
hdfs dfs -chmod -R  777 /user/hive/warehouse/ods/$hive_external_table/dt=$v_externalTablePartition 
beeline -u "jdbc:hive2://10.50.40.4:10000/ods" -n hive -p asdasd110.0 -e "msck repair table ods.ods_bdw_beadwalletloan_activity_discount_distribute"
