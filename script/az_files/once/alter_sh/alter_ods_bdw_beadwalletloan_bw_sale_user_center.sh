
#!/bin/bash
day=`date +%Y%m%d`
beeline -u "jdbc:hive2://10.50.40.4:10000/ods" -n hive -p asdasd110.0 --hivevar date=$day -f /home/dispatch/Project/data-etl-sloth/schedule/etl_script/etl/ods_mysql_hive/ods_alter_table/alter_ods_bdw_beadwalletloan_bw_sale_user_center.sql > /data/disk13/logs/dispatch/logs/sh/alter_ods_bdw_beadwalletloan_bw_sale_user_center.log 2>&1
if [ -f "/data/disk13/logs/dispatch/logs/sh/alter_ods_bdw_beadwalletloan_bw_sale_user_center.log" ];then
result=`grep -nE 'ERROR:|Error ' /data/disk13/logs/dispatch/logs/sh/alter_ods_bdw_beadwalletloan_bw_sale_user_center.log`
if [ ${#result} -gt 0 ];then
exit 22
else exit 0
fi
else 
exit 22
fi
