#!/usr/bin/env bash
basepath=$(cd `dirname $0`; pwd)
kill -9 `cat ${basepath}/az_dispatch_pidfile`
echo "" > /data/disk13/logs/dispatch/logs/run_dispatch.log
if [ -n "$*" ]
then
  nohup java -classpath ${basepath}/../../modules/az_dispatch-1.0-jar-with-dependencies.jar com.beadwallet.exec.DispatchRunner  $* >> /data/disk13/logs/dispatch/logs/run_dispatch.log  2>&1 &  echo $! > ${basepath}/az_dispatch_pidfile
else
  echo "Args[0] is Not Found  Simple : /home/dispatch/Project/data-etl-sloth/conf/config.xml"
fi
