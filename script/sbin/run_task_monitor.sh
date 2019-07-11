#!/usr/bin/env bash
basepath=$(cd `dirname $0`; pwd)
kill -9 `cat ${basepath}/task_monitor_pidfile`
echo "" > /data/disk13/logs/dispatch/logs/run_task_monitor.log
if [ -n "$*" ]
then
  nohup java -classpath ${basepath}/../../modules/task_monitoring-1.0-jar-with-dependencies.jar com.smallelephant.datamodul.TaskMonitorApplication  $* > /data/disk13/logs/dispatch/logs/run_task_monitor.log  2>&1 &  echo $! > ${basepath}/task_monitor_pidfile
else
 echo "Args[0] is Not Found  Simple : /opt/platform/etl_dispatch/conf/config.xml"
fi
