# coding:utf-8
import traceback
import io



# 批量生成文件
def auto_sql(df_columns,df_tables,base_path,log_path,HiveJdbc):
    df = df_columns
    df2 = df_tables
    for table_name_inial in set(df[2]):
        tempp = df.loc[df[2] == table_name_inial]
        temp2 = df2.loc[df2[4] == table_name_inial]
        for table_schema in range(len(temp2[0])):
            temp = tempp.loc[tempp[0]==temp2.iloc[table_schema][0]]
            dbs = temp2.iloc[table_schema][1]+ '_'+temp2.iloc[table_schema][3]
            table_name = "{}_{}".format(dbs, table_name_inial)
            try:
                filepath = "../../script/az_files/once"
                ods_alter_job_state = ods_alter_job(table_name, base_path)
                with io.open('{filepath}/alter_job/alter_ods_{table_name}_flow.job'.format(filepath=filepath,
                                                                                           table_name="{}".format(
                                                                                                   table_name)),
                             'w', encoding='utf8') as f:
                    f.write(ods_alter_job_state)
                ods_alter_sh_state = ods_alter_sh(table_name, base_path,log_path,HiveJdbc)
                with io.open('{filepath}/alter_sh/alter_ods_{table_name}.sh'.format(filepath=filepath,
                                                                                    table_name="{}".format(table_name)),
                             'w', encoding='utf8') as f:
                    f.write(ods_alter_sh_state)

                rds_alter_job_state = rds_alter_job(table_name, base_path)
                with io.open('{filepath}/alter_job/alter_rds_{table_name}_flow.job'.format(filepath=filepath,
                                                                                           table_name="{}".format(
                                                                                               table_name)),
                             'w', encoding='utf8') as f:
                    f.write(rds_alter_job_state)
                rds_alter_sh_state = rds_alter_sh(table_name, base_path,log_path,HiveJdbc)
                with io.open('{filepath}/alter_sh/alter_rds_{table_name}.sh'.format(filepath=filepath,
                                                                                    table_name="{}".format(table_name)),
                             'w', encoding='utf8') as f:
                    f.write(rds_alter_sh_state)
            except Exception as e:
                print("{}: {}\texception{} Error:dbs:{} table:{}".format(__package__,  " ", traceback.format_exc(),dbs,table_name_inial))
                raise


def ods_alter_job(table_name, base_path):
    job = '''
type=command
command=/bin/bash {base_path}/script/az_files/once/alter_sh/alter_ods_{table_name}.sh
failure.emails=redalert-dataplatform@beadwallet.com
'''.format(base_path=base_path, table_name=table_name)
    return job


def ods_alter_sh(table_name, base_path,log_path,HiveJdbc):
    sh = '''
#!/bin/bash
day=`date +%Y%m%d`
beeline -u "{jdbc}" -n {user} -p {passwd} --hivevar date=$day -f {base_path}/script/etl_sql/ods_mysql_hive/ods_alter_table/alter_ods_{table_name}.sql > {log_path}/sh/alter_ods_{table_name}.log 2>&1
if [ -f "{log_path}/sh/alter_ods_{table_name}.log" ];then
result=`grep -nE 'ERROR:|Error ' {log_path}/sh/alter_ods_{table_name}.log`
if [ ${result} -gt 0 ];then
exit 22
else exit 0
fi
else 
exit 22
fi
'''.format(result="{#result}", base_path=base_path, table_name=table_name,log_path=log_path,jdbc=HiveJdbc['jdbc'],user=HiveJdbc['user'],passwd=HiveJdbc['passwd'])
    return sh


def rds_alter_job(table_name, base_path):
    job = '''
type=command
command=/bin/bash {base_path}/script/az_files/once/alter_sh/alter_rds_{table_name}.sh
failure.emails=redalert-dataplatform@beadwallet.com
'''.format(base_path=base_path, table_name=table_name)
    return job


def rds_alter_sh(table_name, base_path,log_path,HiveJdbc):
    sh = '''
#!/bin/bash
day=`date +%Y%m%d`
beeline -u "{jdbc}" -n {user} -p {passwd} --hivevar date=$day -f {base_path}/script/etl_sql/rds_hive_hive/rds_alter_table/alter_rds_{table_name}.sql > {log_path}/sh/alter_rds_{table_name}.log 2>&1
if [ -f "{log_path}/sh/alter_rds_{table_name}.log" ];then
result=`grep -nE 'ERROR:|Error ' {log_path}/sh/alter_rds_{table_name}.log`
if [ ${result} -gt 0 ];then
exit 22
else exit 0
fi
else 
exit 22
fi
'''.format(result="{#result}", base_path=base_path, table_name=table_name,log_path=log_path,jdbc=HiveJdbc['jdbc'],user=HiveJdbc['user'],passwd=HiveJdbc['passwd'])
    return sh


if __name__ == '__main__':
    pass
