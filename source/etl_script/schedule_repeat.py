#coding:utf-8
import traceback
import io
import xml.etree.ElementTree as ET
from urllib import parse
import pymysql
import pandas as pd
import traceback
from datetime import timedelta,datetime
dt = datetime.now()
import xml.etree.ElementTree as ET
from urllib import parse
import os,os.path
import zipfile
from shutil import copy
import logging
logger = logging.getLogger("etl_script")



def parse_xml(xml,tag,db_source,businessSource,dbName):
    tree = ET.ElementTree(file=xml)
    root = tree.getroot()
    root.findall(tag)
    for tag_one in root.findall(tag):
        if tag_one.get('db_source') == db_source and tag_one.get('businessSource') == businessSource and tag_one.get('dbName') == dbName :
            url = tag_one.find('url').text
            jdbc = url
            url = "http://{}".format(url.split("//")[-1])
            url_parsed = parse.urlparse(url)
            host = url_parsed.hostname
            db = url_parsed.path.strip('/')
            user = tag_one.find('user').text
            passwd = tag_one.find('passwd').text

            return  {'jdbc':jdbc,'host':host,'db':db,'user':user,'passwd':passwd}

# 批量生成文件
def auto_sql(AzkabanJdbc,xml_path,df_columns,df_tables,base_path,log_path,HiveJdbc):
    df = df_columns
    df2 = df_tables
    for table_name_inial in set(df[2]):
        tempp = df.loc[df[2] == table_name_inial]
        temp2 = df2.loc[df2[4] == table_name_inial]
        for table_schema in range(len(temp2[0])):
            temp = tempp.loc[tempp[0]==temp2.iloc[table_schema][0]]
            dbs = temp2.iloc[table_schema][1]+ '_'+temp2.iloc[table_schema][3]
            index = temp2.iloc[table_schema][13]
            offset = 0 if temp2.iloc[table_schema][14] is None or pd.isnull(temp2.iloc[table_schema][14]) else temp2.iloc[table_schema][14].astype(int)
            tier = temp2.iloc[table_schema][16]
            time_inst = temp.loc[temp[0] == temp2.iloc[table_schema][0]].loc[temp[7]==1][4]
            time_upd = temp.loc[temp[0] == temp2.iloc[table_schema][0]].loc[temp[8]==1][4]
            column_key = temp.loc[temp[0] == temp2.iloc[table_schema][0]].loc[temp[6]==1][4]
            time_inst = time_inst.values[0] if len(time_inst) != 0 else ""
            time_upd = time_upd.values[0] if len(time_upd) != 0 else ""
            column_key = column_key.values[0] if len(column_key) != 0 else ""
            tag = 'BusinessJdbc'
            table_name = "{}_{}".format(dbs, table_name_inial)
            xml = parse_xml(xml_path,tag,temp2.iloc[table_schema][2],temp2.iloc[table_schema][1],temp2.iloc[table_schema][3])
            xml =xml if xml is not None else  print("Error: table:{} xml is None".format(table_name))
            try:
                filepath = "../../script/az_files"
                ods_etl_job_state = ods_etl_job(table_name,base_path)
                with io.open('{filepath}/repeat/ods_etl_job/etl_ods_{table_name}_flow.job'.format(filepath=filepath, table_name="{}".format(  table_name)),
                          'w',encoding='utf8') as f:
                    f.write(ods_etl_job_state)
                ods_start_sh_state = ods_start_sh(temp2.iloc[table_schema][7],temp2.iloc[table_schema][12], table_name,base_path,log_path)
                with io.open('{filepath}/repeat/ods_etl_sh/start_ods_{table_name}.sh'.format(filepath=filepath,
                                                                        table_name="{}".format( table_name)),
                             'w', encoding='utf8') as f:
                    f.write(ods_start_sh_state)
                ods_etl_sh_state = ods_etl_sh(time_inst,time_upd,xml, temp2.iloc[table_schema][3],table_name,base_path, table_name_inial,HiveJdbc,index,offset,column_key)
                with io.open('{filepath}/repeat/ods_etl_sh/etl_ods_{table_name}.sh'.format(filepath=filepath,
                                                                       table_name="{}".format( table_name)),
                             'w', encoding='utf8') as f:
                    f.write(ods_etl_sh_state)
                if tier != 'ods':
                    rds_etl_job_state = rds_etl_job( table_name,base_path)
                    with io.open('{filepath}/repeat/rds_etl_job/etl_rds_{table_name}_flow.job'.format(filepath=filepath,
                                                                            table_name="{}".format( table_name)),
                                 'w', encoding='utf8') as f:
                        f.write(rds_etl_job_state)
                    rds_etl_sh_state = rds_etl_sh( table_name,base_path,log_path,HiveJdbc)
                    with io.open('{filepath}/repeat/rds_etl_sh/etl_rds_{table_name}.sh'.format(filepath=filepath,
                                                                           table_name="{}".format( table_name)),
                                 'w', encoding='utf8') as f:
                        f.write(rds_etl_sh_state)
                app_check_job_state = app_check_job(table_name, base_path)
                with io.open('{filepath}/app_check/app_check_job/check_app_{table_name}.job'.format(filepath=filepath,
                                                                                        table_name="{}".format(
                                                                                            table_name)),
                             'w', encoding='utf8') as f:
                    f.write(app_check_job_state)
                app_check_sh_state = app_check_sh(AzkabanJdbc, table_name)
                with io.open('{filepath}/app_check/app_check_sh/check_app_{table_name}.sh'.format(filepath=filepath,
                                                                                      table_name="{}".format(
                                                                                          table_name)),
                             'w', encoding='utf8') as f:
                    f.write(app_check_sh_state)
            except Exception as e:
                print("{}: {}\texception{} Error:dbs:{} table:{}".format(__package__,  " ", traceback.format_exc(),dbs,table_name_inial))
                raise


def ods_etl_job(table_name,base_path):
    job = '''
type=command
command=/bin/bash {base_path}/script/az_files/repeat/ods_etl_sh/start_ods_{table_name}.sh
failure.emails=redalert-dataplatform@beadwallet.com
'''.format(base_path=base_path,table_name=table_name)
    return job


def ods_start_sh(time_offset,table_create,table_name,base_path,log_path):
    sh = '''
#!/bin/bash
v_externalTablePartition=`date +%Y%m%d`
hdfs dfs -test -e /user/hive/warehouse/ods/ods_{table_name}/dt=$v_externalTablePartition
if [ $? -eq 0 ];then
echo "exsit"
hdfs dfs -rm -r /user/hive/warehouse/ods/ods_{table_name}/dt=$v_externalTablePartition
else 
echo "not exsit"
fi
if [ '{table_create}' = $v_externalTablePartition ];then 
 start_dt=2016-01-01
 end_dt=`date '+%F '"{time_offset}:00:00"`
else start_dt=`date -d"-1 day" '+%F '"{time_offset}:00:00"` end_dt=`date '+%F '"{time_offset}:00:00"`
fi 
sh {base_path}/script/az_files/repeat/ods_etl_sh/etl_ods_{table_name}.sh "$start_dt" "$end_dt"  > {log_path}/sh/etl_ods_{table_name}.log 2>&1
if [ -f "{log_path}/sh/etl_ods_{table_name}.log" ];then
result=`grep -nE 'ERROR|Error ' {log_path}/sh/etl_ods_{table_name}.log`
if [ ${result} -gt 0 ];then
exit 22
else exit 0
fi
else 
exit 22
fi

'''.format(base_path=base_path,result="{#result}",table_create=table_create,time_offset=time_offset if time_offset > 9 else '0{}'.format(time_offset),table_name=table_name,log_path=log_path)
    return sh


def ods_etl_sh(time_inst,time_upd,xml,dbs,table_name,base_path, table_name_inial,HiveJdbc,index,offset,column_key):
    sql = '''"$sqltemp where a.{column_key}>{offset}  and "\$CONDITIONS""'''.format(column_key=column_key,offset=offset) if index != 0 else (
        '''"$sqltemp where (a.{time_inst}>="\\"$v_dt\\"" and a.{time_inst}< "\\"$end_dt\\"") or (a.{time_upd}>="\\"$v_dt\\"" and a.{time_upd}< "\\"$end_dt\\"") and "\$CONDITIONS""'''.format(time_inst=time_inst,time_upd=time_upd) if time_upd != "" and time_inst !="" else (
        '''"$sqltemp where a.{time_inst}>="\\"$v_dt\\"" and a.{time_inst}< "\\"$end_dt\\"" and "\$CONDITIONS""'''.format(
            time_inst=time_inst) if time_upd == "" and time_inst !="" else ('''"$sqltemp where a.{time_upd}>="\\"$v_dt\\"" and a.{time_upd}< "\\"$end_dt\\"" and "\$CONDITIONS""'''.format(
            time_upd=time_upd) if time_upd != "" and time_inst =="" else '')))

    sh = '''#!/bin/bash
CONNECTURL='{CONNECTURL}'
USERNAME={USERNAME}
PASSWORD={PASSWORD}
#table.conf
mysql_database={dbs}
mysql_table={table_name_inial}
hive_external_table=ods_{table_name}

sqltemp=`cat {base_path}/script/etl_sql/ods_mysql_hive/ods_regular_table/etl_ods_{table_name}.sql`
v_dt=$1
end_dt=$2
#下面是基础查询语句 拼接 where条件
sql={sql}
#分区指定为今日
 v_externalTablePartition=`date +%Y%m%d`
 

sqoop import \
--connect $CONNECTURL \
--username $USERNAME \
--password $PASSWORD \
--query "$sql" \
--target-dir "/user/hive/warehouse/ods/"$hive_external_table"/"$v_externalTablePartition \
--num-mappers 1 \
--as-parquetfile \
--driver com.mysql.jdbc.Driver \

hdfs dfs -mv /user/hive/warehouse/ods/$hive_external_table/$v_externalTablePartition /user/hive/warehouse/ods/$hive_external_table/dt=$v_externalTablePartition 
hdfs dfs -chmod -R  777 /user/hive/warehouse/ods/$hive_external_table/dt=$v_externalTablePartition 
beeline -u "{jdbc}" -n {user} -p {passwd} -e "msck repair table ods.ods_{table_name}"
'''.format(sql=sql,CONNECTURL=xml['jdbc'],USERNAME=xml['user'],PASSWORD=xml['passwd'],dbs=dbs,table_name_inial=table_name_inial,base_path=base_path,table_name=table_name,jdbc=HiveJdbc['jdbc'],user=HiveJdbc['user'],passwd=HiveJdbc['passwd'])
    return sh


def rds_etl_sh(table_name,base_path,log_path,HiveJdbc):
    sh = '''
#!/bin/bash
day=`date +%Y%m%d`
beeline -u "{jdbc}" -n {user} -p {passwd} --hivevar date=$day -f {base_path}/script/etl_sql/rds_hive_hive/rds_regular_table/etl_rds_{table_name}.sql > {log_path}/sh/etl_rds_{table_name}.log 2>&1
if [ -f "{log_path}/sh/etl_rds_{table_name}.log" ];then
result=`grep -nE 'ERROR|Error ' {log_path}/sh/etl_rds_{table_name}.log`
if [ ${result} -gt 0 ];then
exit 22
else exit 0
fi
else 
exit 22
fi
'''.format(result="{#result}",base_path=base_path,table_name=table_name,log_path=log_path,jdbc=HiveJdbc['jdbc'],user=HiveJdbc['user'],passwd=HiveJdbc['passwd'])
    return sh


def rds_etl_job(table_name,base_path):
    job = '''
type=command
command=/bin/bash {base_path}/script/az_files/repeat/rds_etl_sh/etl_rds_{table_name}.sh
dependencies=etl_ods_{table_name}_flow
failure.emails=redalert-dataplatform@beadwallet.com
'''.format(base_path=base_path,table_name=table_name)
    return job


def app_check_job(table_name,base_path):
    job = '''
type=command
command=/bin/bash {base_path}/script/az_files/app_check/app_check_sh/check_app_{table_name}.sh
retries=12
retry.backoff=300000
failure.emails=redalert-dataplatform@beadwallet.com
'''.format(base_path=base_path,table_name=table_name)
    return job


def app_check_sh(AzkabanJdbc, table_name):
    sh = '''
#!/bin/bash
load_dt=$(date -d"0 day" "+%Y%m%d")
check_sql="SELECT count(1) FROM (SELECT t1.status,t1.flow_id,t1.project_id FROM execution_flows t1 INNER JOIN projects t2 ON t1.project_id = t2.id WHERE FROM_UNIXTIME( t1.start_time / 1000, '%Y%m%d' ) = '$load_dt' AND t2.active = 1 AND t1.flow_id = 'etl_rds_{table_name}_flow' ORDER BY t1.update_time DESC LIMIT 1) t WHERE t.status = 50;"
sql_result=`mysql -h{host} -P{port} -u{user} -p{passwd} azkaban -e "${check_sql}"`
check_result=`echo $sql_result | awk -F" " '{print}'`
if [ $check_result -ne 1 ];then
exit 11
echo "not enough"
else exit 0
fi
'''.format(host=AzkabanJdbc['host'],port=AzkabanJdbc['port'],user=AzkabanJdbc['user'],passwd=AzkabanJdbc['passwd'],print="{print $2}",check_sql="{check_sql}",table_name=table_name)
    return sh



if __name__ == '__main__':
    xml = os.path.abspath(os.path.join(os.getcwd(), "../../../conf/config.xml"))
