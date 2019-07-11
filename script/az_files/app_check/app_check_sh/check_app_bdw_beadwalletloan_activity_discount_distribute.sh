
#!/bin/bash
load_dt=$(date -d"0 day" "+%Y%m%d")
check_sql="SELECT count(1) FROM (SELECT t1.status,t1.flow_id,t1.project_id FROM execution_flows t1 INNER JOIN projects t2 ON t1.project_id = t2.id WHERE FROM_UNIXTIME( t1.start_time / 1000, '%Y%m%d' ) = '$load_dt' AND t2.active = 1 AND t1.flow_id = 'etl_rds_bdw_beadwalletloan_activity_discount_distribute_flow' ORDER BY t1.update_time DESC LIMIT 1) t WHERE t.status = 50;"
sql_result=`mysql -h10.50.40.1 -P3306 -udispatch -pdispatch2019 azkaban -e "${check_sql}"`
check_result=`echo $sql_result | awk -F" " '{print $2}'`
if [ $check_result -ne 1 ];then
exit 11
echo "not enough"
else exit 0
fi
