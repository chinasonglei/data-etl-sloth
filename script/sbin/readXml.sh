#!/usr/bin/env bash


## 需要查询的参数

businessSource="metadata"
#businessSource="$1"

dbName="test"

#dbName="$2"

array=(`cat ../../conf/config.xml| grep "BusinessJdbc" -n | awk '{print$1}' | sed s/://g | awk '{print$1}'`)
dstLine=`cat ../conf/config.xml | grep  businessSource=\"${businessSource}\"  -n | grep  dbName=\"${dbName}\"  | awk '{print$1}' | sed s/://g | awk '{print$1}'`
f=0
for j in $(seq 1 `expr ${#array[*]} / 2`)
do
    start=`expr $f`
    end=`expr $f + 1`
      if [ "${dstLine}" -eq "${array[$start]}" ];then
            username=`sed -n ${array[$start]},${array[$end]}p ../conf/config.xml | grep username | awk -F ">" '{print $2}' | awk -F "<" '{print $1}'`
            password=`sed -n ${array[$start]},${array[$end]}p ../conf/config.xml | grep password | awk -F ">" '{print $2}' | awk -F "<" '{print $1}'`
            url=`sed -n ${array[$start]},${array[$end]}p ../conf/config.xml | grep url | awk -F ">" '{print $2}' | awk -F "<" '{print $1}'`

            url1=`echo ${url} | grep -Eoe "(([0-9]|[1-9][0-9]|1[0-9]{2}|2[0-4][0-9]|25[0-5])\.){3}([0-9]|[1-9][0-9]|1[0-9]{2}|2[0-4][0-9]|25[0-5])" -Eoe '^(e|b)[[:lower:]]+[[:digit:]]+?'`

            echo "${username} ${password} ${url1}"
      fi
    ((f=f+2))
done
