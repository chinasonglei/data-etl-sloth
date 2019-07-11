#!/bin/bash
load_dt=$(date -d"0 day" "+%Y%m%d")
check_sql="select count(*) from meta_execute_records where \`date\`='$load_dt' and \`module\` = 'meta_manage' and \`status\` = 1;"
sql_result=`mysql -h10.50.40.1 -P3306 -udispatch -pdispatch2019 dispatch -e "${check_sql}"`
check_result=`echo $sql_result | awk -F" " '{print $2}'`
if [ $check_result -ne 1 ];then
exit 11
echo "not enough"
else exit 0
fi