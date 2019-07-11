
import pandas as pd
import traceback
import io
import re



# 类型转换
def type_conversion(type_name, type_dict, source,target):
    # type_name = "timestamp"
    parttern = re.compile(r"[a-z]+", re.I)  # 匹配英文

    # 匹配字典中类型映射
    type_name_in_hive = type_dict[type_dict[source]==parttern.match(type_name).group(0)][target].values[0] if len(type_dict[type_dict[source]==parttern.match(type_name).group(0)][target]) !=0 else print("Error:{}  column_type is None 'type_dict'".format(type_name))

    return type_name_in_hive


# 批量生成文件
def auto_sql(df_columns,df_tables,type_dict,column_source,column_target):
    df = df_columns
    df2 = df_tables
    for table_name_inial in set(df[2]):
        tempp = df.loc[df[2] == table_name_inial]
        temp2 = df2.loc[df2[4] == table_name_inial]
        for table_schema in range(len(temp2[0])):
            temp = tempp.loc[tempp[0]==temp2.iloc[table_schema][0]]
            dbs = temp2.iloc[table_schema][1]+ '_'+temp2.iloc[table_schema][3]
            table_name = "{}_{}".format(dbs, table_name_inial)
            table_name_all = '''rds.rds_{}'''.format(table_name)
            table_name_source = '''ods.ods_{}'''.format(table_name)
            try:
                sql = rds_regular_sql(temp, table_name_all,table_name_source,type_dict,column_source,column_target)
                filepath = "../../script/etl_sql/rds_hive_hive/rds_regular_table"
                with io.open('{filepath}/etl_rds_{table_name}.sql'.format(filepath=filepath, table_name=table_name),
                          'w',encoding='utf8') as f:
                    f.write(sql)
            except Exception as e:
                print("{}: {}\texception{} Error:dbs:{} table:{}".format(__package__,  " ", traceback.format_exc(),dbs,table_name_inial))
                raise


def rds_regular_sql(temp, table_name_all,table_name_source,type_dict,column_source,target):
    # 匹配类型包含decimal的
    parttern = re.compile(r"decimal", re.I)

    a_lis =[]
    c_lis =[]
    for i in list(temp[4]):
        a_lis.append("A.`{}`".format(i))
        c_lis.append("C.`{}`".format(i) if not parttern.search(temp[temp[4]==i][5].values[0]) else '''CAST(C.{} as {})'''.format(i, type_conversion(temp[temp[4] == i][5].values[0], type_dict, column_source,target)))
    a_lis = ",\n".join(a_lis)
    c_lis = ",\n".join(c_lis)

    null_lis =  ['''-2147483648''' if is_column_key == 1 else 'null' for is_column_key,is_time_inst in temp[[6,7]].itertuples(index=False, name=False)]
    null_lis = ",\n".join(null_lis)

    column_key = temp[temp[6]==1][4].values[0] if len(temp[temp[6]==1][4]) !=0 else  print("Error: table:{} column_key is None".format(table_name_source))

    sql = '''
SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.exec.max.dynamic.partitions=200000;
SET hive.exec.max.dynamic.partitions.pernode=20000;
SET parquet.memory.min.chunk.size=100000;
SET hive.execution.engine=mr;
SET hive.mapjoin.smalltable.filesize=55000000;
SET hive.auto.convert.join=false;
set hive.optimize.sort.dynamic.partition = true;
set mapred.job.reuse.jvm.num.tasks=20;
set hive.exec.reducers.bytes.per.reducer=150000000;
set hive.exec.parallel=true;
set mapreduce.map.memory.mb=12288;
set mapreduce.reduce.memory.mb=8192;
set hive.exec.stagingdir=./temp;

    
INSERT OVERWRITE table {table_name_all}
partition(is_valid,ce_start_date,ce_end_date)
SELECT * FROM
(
SELECT
{a_lis},
CASE WHEN B.{column_key} IS NOT NULL THEN 0 ELSE A.is_valid
END AS is_valid,
A.ce_start_date,
CASE WHEN B.{column_key} IS NOT NULL THEN B.dt
ELSE A.ce_end_date
END AS ce_end_date
FROM (select * from {table_name_all} where is_valid=1) AS A
LEFT JOIN {table_name_source} AS B
ON A.{column_key}=B.{column_key} and B.dt=${date}
union
SELECT
{c_lis},
1 AS is_valid,
${date} AS ce_start_date,
99991231 AS ce_end_date
FROM {table_name_source} AS C
where C.dt=${date}
)AS T;

insert into table {table_name_all} 
partition(is_valid,ce_start_date,ce_end_date)
select
{null_lis},
1,
${date},
99991231;
'''.format(table_name_source=table_name_source,column_key=column_key,a_lis=a_lis,c_lis=c_lis,date="{date}",table_name_all=table_name_all,null_lis=null_lis)

    # print(sql)
    return sql


if __name__ == '__main__':
    pass
