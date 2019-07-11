
import configparser
import re
import pandas as pd
import traceback
import io
import os,os.path


conf = configparser.ConfigParser()
conf.read('meta_data/config.ini')


# 类型转换
def type_conversion(type_name, type_dict, source,target):
    # type_name = "timestamp"
    parttern = re.compile(r"[a-z]+", re.I)  # 匹配英文

    # 匹配字典中类型映射
    type_name_in_hive = type_dict[type_dict[source]==parttern.match(type_name).group(0)][target].values[0] if len(type_dict[type_dict[source]==parttern.match(type_name).group(0)][target]) !=0 else print("Error: {}  column_type is None 'type_dict'".format(type_name))
    if type_name_in_hive == "decimal" and target == 2:
        type_name_in_hive = type_name
    return type_name_in_hive


# 批量生成文件
def auto_sql(df_columns,df_tables,  type_dict,column_source,column_target):
    df = df_columns
    df2 = df_tables
    for table_name_inial in set(df[2]):
        tempp = df.loc[df[2] == table_name_inial]
        temp2 = df2.loc[df2[4] == table_name_inial]
        for table_schema in range(len(temp2[0])):
            temp = tempp.loc[tempp[0]==temp2.iloc[table_schema][0]]
            dbs = temp2.iloc[table_schema][1]+ '_'+temp2.iloc[table_schema][3]
            table_name = "{}_{}".format(dbs, table_name_inial)
            table_name_new = '''ods_{}'''.format(table_name)
            table_name_all = '''ods.ods_{}'''.format(table_name)
            try:
                sql = ods_create_sql(temp, temp2, table_name_all, type_dict, column_source,column_target,table_name_new)
                filepath = "../../script/etl_sql/ods_mysql_hive/ods_create_table"
                with io.open('{filepath}/create_{table_name}.sql'.format(filepath=filepath, table_name=table_name_new),
                          'w',encoding='utf8') as f:
                    f.write(sql)
            except Exception as e:
                print("{}: {}\texception{} Error:dbs:{} table:{}".format(__package__,  " ", traceback.format_exc(),dbs,table_name_inial))
                raise


def ods_create_sql(temp, temp2, table_name_all, type_dict, column_source,column_target,table_name_new):

    drop = "drop table if exists {} ;\n".format(table_name_all)

    lis = ["`{}` {} comment '{}'".format(varname.lower(), type_conversion(vartype, type_dict, column_source,column_target),column_comment) for varname, vartype,column_comment in temp[[4,5,9]].itertuples(index=False, name=False)]
    lis = ",\n".join(lis)

    sql = "{drop}create table {table_name} (\n" \
          "{create_col} )" \
          "comment '{table}'" \
          "partitioned by (dt int) stored as parquet " \
          "location 'hdfs:///user/hive/warehouse/ods/{table_name_new}' " \
          ";".format(drop=drop, table_name_new=table_name_new,table_name=table_name_all, create_col=lis, table=list(temp2[6])[0])

    # print(sql)
    return sql


if __name__ == '__main__':
    pass
