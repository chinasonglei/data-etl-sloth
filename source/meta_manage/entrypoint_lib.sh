#!/usr/bin/env bash
# owner 张建新

basepath=$(cd `dirname $0`; pwd)

export  CLASSPATH=`find lib/ -iname '*.jar'  | xargs | sed  "s/ /:/g"`

nohup java -cp $CLASSPATH  com.beadwallet.metadata.MetadataApplication $* >> ${basepath}/logs/MetadataApplication.log  2>&1 &  echo $! > ${basepath}/pid/MetadataApplication_Pidfile
