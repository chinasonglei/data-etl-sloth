#!/usr/bin/env bash
basepath=$(cd `dirname $0`; pwd)
kill -9 `jps |grep 'meta_manage'|awk '{print $1}'`
dt=`date +%Y-%m-%d`
if [ -n "$*" ]
then
  echo "" > /data/disk13/logs/dispatch/logs/run_meta_data.log
  echo "" > /data/disk13/logs/dispatch/logs/${dt}.log
  nohup java -jar ${basepath}/../../modules/meta_manage-1.0.jar $* >> /data/disk13/logs/dispatch/logs/run_meta_data.log  2>&1 &  echo $! > ${basepath}/metadata_pidfile
  if [ ${#result} -gt 0 ];then
    exit 22
  fi
else
  echo "Args[0] is Not Found  Simple : /home/dispatch/platform/etl_dispatch/conf/config.xml"
fi
