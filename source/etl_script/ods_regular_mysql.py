
import traceback
import io
import re


# 批量生成文件
def auto_sql(df_columns,df_tables):
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
            try:
                sql = ods_regular_sql(temp, table_name_inial)
                filepath = "../../script/etl_sql/ods_mysql_hive/ods_regular_table"
                with io.open('{filepath}/etl_{table_name}.sql'.format(filepath=filepath, table_name=table_name_new),
                          'w',encoding='utf8') as f:
                    f.write(sql)
            except Exception as e:
                print("{}: {}\texception{} Error:dbs:{} table:{}".format(__package__,  " ", traceback.format_exc(),dbs,table_name_inial))
                raise


def ods_regular_sql(temp, table_name_all):
    # 匹配类型包含date/point/time的
    lis = [ '''`{}`'''.format(varname.lower()) if len(re.findall(r"date|time|point",vartype,))==0
            else {'date':'''DATE_FORMAT({varname}, "%Y-%m-%d %H:%i:%s") AS {varname}'''.format(varname=varname.lower()),
                  'time':'''DATE_FORMAT({varname}, "%Y-%m-%d %H:%i:%s") AS {varname}'''.format(varname=varname.lower()),
                  'point':'''astext({varname}) as {varname}'''.format(varname=varname.lower())}.get(re.findall(r"date|time|point",vartype,)[0])
            for varname, vartype,column_comment in temp[[4,5,9]].itertuples(index=False, name=False)]
    lis = ",\n".join(lis)

    sql = "select \n" \
          "{create_col}\n" \
          " from {table_name} a".format(table_name=table_name_all, create_col=lis)

    # print(sql)
    return sql


if __name__ == '__main__':
    pass
