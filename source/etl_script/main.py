# coding:utf-8
#!/usr/bin/python

import pymysql
import pandas as pd
import numpy as np
from datetime import timedelta,datetime
dt = datetime.now()
import xml.etree.ElementTree as ET
from urllib import parse
import os,os.path
import zipfile
from shutil import copy
import logging
logger = logging.getLogger("etl_script")

from ods_create_mysql import auto_sql as ods_create_auto
from ods_regular_mysql import auto_sql as ods_regular_auto
from rds_create_mysql import auto_sql as rds_create_auto
from rds_regular_mysql import auto_sql as rds_regular_auto
from schedule_create import auto_sql as schedule_create_auto
from ods_alter_mysql import auto_sql as ods_alter_auto
from rds_alter_mysql import auto_sql as rds_alter_auto
from schedule_alter import auto_sql as schedule_alter_auto
from schedule_repeat import auto_sql as schedule_repeat_auto





def zip_dir(dirname, zipfilename):
    filelist = []
    if os.path.isfile(dirname):
        filelist.append(dirname)
    else:
        for root, dirs, files in os.walk(dirname):
            for name in files:
                filelist.append(os.path.join(root, name))

    zf = zipfile.ZipFile(zipfilename, "w", zipfile.zlib.DEFLATED)
    for tar in filelist:
        arcname = tar[len(dirname):]
        # print arcname
        zf.write(tar, arcname)
    zf.close()

def load_metadata_mysql(conn_metadate,sql):
    conn =pymysql.connect(conn_metadate['host'],conn_metadate['user'],conn_metadate['passwd'],conn_metadate['db'])
    cursor = conn.cursor()
    cursor.execute(sql)
    value = cursor.fetchall()
    cursor.close()

    return value

def parse_xml(xml,tag):
    tree = ET.ElementTree(file=xml)
    url = tree.find('{}/url'.format(tag)).text
    dispatchHome = tree.find('Dispatch/dispatchHome').text
    log_path = tree.find('LogServerConf/path').text
    jdbc = url
    url = "http://{}".format(url.split("//")[-1])
    url_parsed = parse.urlparse(url)
    host = url_parsed.hostname if tag!='HiveJdbc' else ""
    port = url_parsed.port if tag!='HiveJdbc' else ""
    db = url_parsed.path.strip('/')
    user = tree.find('{}/user'.format(tag)).text
    passwd = tree.find('{}/passwd'.format(tag)).text

    return  {'jdbc':jdbc,'port':port,'host':host,'db':db,'user':user,'passwd':passwd,'dispatchHome':dispatchHome,'log_path':log_path}

def judge(parse_xml,create_or_alter,df_sql):
    if load_metadata_mysql(parse_xml,create_or_alter) != 0 :
        once_df_tables = pd.DataFrame(list(load_metadata_mysql(parse_xml,'{df_sql}'.format(df_sql=df_sql))))
    else :
        once_df_tables = pd.DataFrame(columns=['a','b'])

    return once_df_tables

if __name__ == '__main__':

    xml = os.path.abspath(os.path.join(os.getcwd(), "../../conf/config.xml"))
    log_path = parse_xml(xml, 'Metastore')['log_path']
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(name)-12s %(levelname)s %(message)s',
                        datefmt='%Y-%m-%d %H:%M:%S', filename='{log_path}'.format(log_path=log_path + '/'+ dt.date().strftime('%Y-%m-%d') + '.log'), filemode='a')
    try:
        for root, dirs, files in os.walk(os.path.abspath(os.path.join(os.getcwd(), "../../script/az_files")) , topdown=False):
            for name in files:
                os.remove(os.path.join(root, name))
        logger.info('Delete schedule file success ')

        today = (datetime.today()).strftime('%Y%m%d')
        rds_ods_columns = pd.DataFrame(list(load_metadata_mysql(parse_xml(xml,'Metastore'),'''select id,db_name,`table_name`,ordinal_position,`column_name`,column_type,column_key,is_create_time,is_update_time,column_comment,sensitive_data,status,busi_column,hive_column,update_time from data_schema_detail order by ordinal_position;''')))
        ods_busi_columns = pd.DataFrame(list(load_metadata_mysql(parse_xml(xml,'Metastore'),'''select id,db_name,`table_name`,ordinal_position,`column_name`,column_type,column_key,is_create_time,is_update_time,column_comment,sensitive_data,status,busi_column,hive_column,update_time from data_schema_detail where busi_column = 1 order by ordinal_position;''')))
        alter_columns = pd.DataFrame(list(load_metadata_mysql(parse_xml(xml,'Metastore'),'''select id,db_name,`table_name`,ordinal_position,`column_name`,column_type,column_key,is_create_time,is_update_time,column_comment,sensitive_data,status,busi_column,hive_column,update_time from data_schema_detail where update_time = {}  order by ordinal_position;'''.format(today))))
        repeat_df_tables = pd.DataFrame(list(load_metadata_mysql(parse_xml(xml, 'Metastore'), '''select a.id,a.business_source,a.db_source,a.db_name,a.table_name,a.data_length,a.table_comment,a.time_offset,a.last_update,a.current_ddl,a.load2hive,a.delete,a.storage,a.use_specified_index,b.offset ,b.insert_time,a.tier from (
        select * from data_dictionary where load2hive = 1 and `delete` = 0 and db_source = 'MySQL') a 
        left join (
        select * from (select * from `index_offset_records` order by `insert_time` desc) c  group by id ) b 
        on b.id = a.id ;''')))

        is_create = "select count(1) from data_dictionary where load2hive = 1 and storage = '{}' and db_source = 'MySQL' and `delete` = 0;".format(today)
        is_alter = '''select count(1) from data_dictionary where load2hive = 1 and last_update = '{}' and DATE_FORMAT(last_update,"%Y%m%d")!=`storage` and db_source = 'MySQL' and `delete` = 0;'''.format(today)
        create_df_sql = "select id,business_source,db_source,db_name,`table_name`,data_length,table_comment,time_offset,last_update,current_ddl,load2hive,`delete`,storage from data_dictionary where load2hive = 1 and storage = '{}'  and `delete` = 0 and db_source = 'MySQL';".format(today)
        alter_df_sql = '''select id,business_source,db_source,db_name,`table_name`,data_length,table_comment,time_offset,last_update,current_ddl,load2hive,`delete`,storage from data_dictionary where load2hive = 1 and last_update = '{}' and DATE_FORMAT(last_update,"%Y%m%d")!=`storage`  and `delete` = 0 and db_source = 'MySQL';'''.format(today)
        type_df_sql = "select mysql,ods,rds from data_type_dic ;"
        type_dict = judge(parse_xml(xml,'Metastore'),type_df_sql,type_df_sql)
        jdbc = parse_xml(xml, 'Metastore')['jdbc']
        HiveJdbc = parse_xml(xml,'HiveJdbc')
        base_path = parse_xml(xml,'Metastore')['dispatchHome']
        create_df_tables = judge(parse_xml(xml,'Metastore'),is_create,create_df_sql)
        if create_df_tables.empty != True:
            schedule_create_auto(ods_busi_columns, create_df_tables,base_path,log_path,HiveJdbc)
            logger.info('schedule_create success')

        alter_df_tables = judge(parse_xml(xml,'Metastore'),is_alter,alter_df_sql)
        if alter_df_tables.empty != True:
            ods_alter_auto(alter_columns,alter_df_tables,type_dict,0,1)
            logger.info('ods_alter_sql success')
            rds_alter_auto(alter_columns,alter_df_tables,type_dict,0,2)
            logger.info('rds_alter_sql success')
            schedule_alter_auto(ods_busi_columns, alter_df_tables,base_path,log_path,HiveJdbc)
            logger.info('schedule_alter success')

        ods_create_auto(ods_busi_columns,repeat_df_tables,type_dict,0,1)
        logger.info('ods_create_sql success')
        rds_create_auto(rds_ods_columns,repeat_df_tables.loc[repeat_df_tables[16]!='ods'],type_dict,0,2)
        logger.info('rds_create_sql success')
        ods_regular_auto(ods_busi_columns, repeat_df_tables)
        logger.info('ods_regular_sql success')
        rds_regular_auto(rds_ods_columns, repeat_df_tables,type_dict,0,2)
        logger.info('rds_regular_sql success')
        schedule_repeat_auto(parse_xml(xml,'AzkabanJdbc'),xml,rds_ods_columns, repeat_df_tables, base_path,log_path,HiveJdbc)
        logger.info('schedule_repeat success')

        dt_str = dt.strftime( '%Y%m%d%H%M%S' )
        zip_dir('../../script/az_files/once/','../../script/az_files/once/data_once_{}.zip'.format(dt_str))
        logger.info('data_once_{}.zip success'.format(dt_str))
        zip_dir('../../script/az_files/repeat/','../../script/az_files/repeat/data_etl_{}.zip'.format(dt_str))
        logger.info('data_etl_{}.zip success'.format(dt_str))
        copy('../../script/az_files/once/data_once_{}.zip'.format(dt_str),'../../script/az_files_history/')
        logger.info('copy data_once_{}.zip success'.format(dt_str))
        copy('../../script/az_files/repeat/data_etl_{}.zip'.format(dt_str),'../../script/az_files_history/')
        logger.info('copy data_etl_{}.zip success'.format(dt_str))
    except Exception as e:
        logger.error(e)
