
#!/bin/bash
day=`date +%Y%m%d`
beeline -u "jdbc:hive2://10.50.40.4:10000/ods" -n hive -p asdasd110.0 --hivevar date=$day -f /home/dispatch/Project/data-etl-sloth/schedule/etl_script/etl/rds_hive_hive/rds_regular_table/etl_rds_bdw_beadwalletloan_activity_discount_useage.sql > /data/disk13/logs/dispatch/logs/sh/etl_rds_bdw_beadwalletloan_activity_discount_useage.log 2>&1
if [ -f "/data/disk13/logs/dispatch/logs/sh/etl_rds_bdw_beadwalletloan_activity_discount_useage.log" ];then
result=`grep -nE 'ERROR:|Error ' /data/disk13/logs/dispatch/logs/sh/etl_rds_bdw_beadwalletloan_activity_discount_useage.log`
if [ ${#result} -gt 0 ];then
exit 22
else exit 0
fi
else 
exit 22
fi
