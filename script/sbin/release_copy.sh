#!/usr/bin/env bash
BASH_PATH=$(cd `dirname $0`; pwd)
HOME=`cat ${BASH_PATH}/../conf/autocp.conf | grep ETL_HOME | awk -F "=" '{print$2}'`
PASS=`cat ${BASH_PATH}/../conf/autocp.conf | grep PASS | awk -F "=" '{print$2}'`
USER=`cat ${BASH_PATH}/../conf/autocp.conf | grep USER | awk -F "=" '{print$2}'`
ARRAYS=(`cat ${BASH_PATH}/../conf/autocp.conf | grep HOST | awk -F "=" '{print$2}'`)

for i in $(seq 0 `expr ${#ARRAYS[*]} - 1`)
do
  echo "${USER}@${ARRAYS[i]} start"
  sshpass -p ${PASS} ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null ${USER}@${ARRAYS[i]} "mkdir -p ${HOME}/conf"
  sshpass -p ${PASS} scp -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null ${HOME}/conf/* ${USER}@${ARRAYS[i]}:${HOME}/conf

  sshpass -p ${PASS} ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null ${USER}@${ARRAYS[i]} "mkdir -p ${HOME}/jar"
  sshpass -p ${PASS} scp -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null ${HOME}/jar/*.jar ${USER}@${ARRAYS[i]}:${HOME}/jar

  sshpass -p ${PASS} ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null ${USER}@${ARRAYS[i]} "mkdir -p ${HOME}/schedule/etl_script/python_scripts"
  sshpass -p ${PASS} scp -r -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null ${HOME}/schedule/etl_script/python_scripts ${USER}@${ARRAYS[i]}:${HOME}/schedule/etl_script

  sshpass -p ${PASS} ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null ${USER}@${ARRAYS[i]} "mkdir -p ${HOME}/sbin"
  sshpass -p ${PASS} scp -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null ${HOME}/sbin/*.sh ${USER}@${ARRAYS[i]}:${HOME}/sbin

  echo "${USER}@${ARRAYS[i]} end"
done
