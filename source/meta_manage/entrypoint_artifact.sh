#!/usr/bin/env bash
# owner 张建新

basepath=$(cd `dirname $0`; pwd)


if [ -n "$*" ]
then
        mkdir -p ${basepath}/logs
        mkdir -p ${basepath}/pid
        nohup java -jar ${basepath}/metadata-0.0.1-SNAPSHOT.jar $* >> ${basepath}/logs/running.log  2>&1 &  echo $! > ${basepath}/pidfile

else
       echo "Args[0] is Not Found  Simple : /opt/platform/dispatch_test/conf/config.xml"

fi


