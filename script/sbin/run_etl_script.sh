#!/usr/bin/env bash
basepath=$(cd `dirname $0`; pwd)

cd ${basepath}/../../modules/etl_script/
echo "" > /data/disk13/logs/dispatch/logs/run_etl_script.log
nohup /opt/anaconda3/bin/python main.py >> /data/disk13/logs/dispatch/logs/run_etl_script.log  2>&1
if [ ${#result} -gt 0 ];then
exit 22
else
scp -r /home/dispatch/Project/data-etl-sloth/script @10.50.40.9:/home/dispatch/Project/data-etl-sloth/  
scp -r /home/dispatch/Project/data-etl-sloth/script @10.50.40.7:/home/dispatch/Project/data-etl-sloth/  
scp -r /home/dispatch/Project/data-etl-sloth/script @10.50.40.8:/home/dispatch/Project/data-etl-sloth/  
fi
