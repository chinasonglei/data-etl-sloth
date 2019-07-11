
import re
import pandas as pd
import traceback
import io




# 类型转换
def type_conversion(type_name, type_dict, source,target):
    # type_name = "timestamp"
    parttern = re.compile(r"[a-z]+", re.I)  # 匹配英文

    # 匹配字典中类型映射
    type_name_in_hive = type_dict[type_dict[source]==parttern.match(type_name).group(0)][target].values[0] if len(type_dict[type_dict[source]==parttern.match(type_name).group(0)][target]) !=0 else print("Error:{}  column_type is None 'type_dict'".format(type_name))

    return type_name_in_hive

# 批量生成文件
def auto_sql(df_columns,df_tables,  type_dict,column_source,target):
    df = df_columns
    df2 = df_tables
    for table_name_inial in set(df[2]):
        tempp = df.loc[df[2] == table_name_inial]
        temp2 = df2.loc[df2[4] == table_name_inial]
        for table_schema in range(len(temp2[0])):
            temp = tempp.loc[tempp[0]==temp2.iloc[table_schema][0]]
            dbs = temp2.iloc[table_schema][1]+ '_'+temp2.iloc[table_schema][3]
            table_name = "{}_{}".format(dbs, table_name_inial)
            table_name_new = '''rds_{}'''.format(table_name)
            table_name_all = '''rds.rds_{}'''.format(table_name)
            try:
                sql = rds_alter_sql(temp, table_name_new, type_dict, column_source,target,dbs,table_name_inial)
                filepath = "../../script/etl_sql/rds_hive_hive/rds_alter_table"
                with io.open('{filepath}/alter_{table_name}.sql'.format(filepath=filepath, table_name=table_name_new),
                          'w',encoding='utf8') as f:
                    f.write(sql)
            except Exception as e:
                print("{}: {}\texception{} Error:dbs:{} table:{}".format(__package__,  " ", traceback.format_exc(),dbs,table_name_inial))
                raise

def rds_alter_sql(temp, table_name_all, type_dict, column_source,target,dbs,table_name_inial):

    add_lis =[]
    change_lis =[]
    add_count = 0
    change_count = 0
    for i in list(temp[4]):
        if temp[temp[4] == i][11].values[0] == 1 :
            add_count += 1
            add_lis.append("`{}` {} comment '{}'".format(i.lower(), type_conversion(temp[temp[4] == i][5].values[0] if len(temp[temp[4] == i][5]) !=0 else print("Error: table:{} column_type is None".format(table_name_all)), type_dict, column_source,target),temp[temp[4] == i][9].values[0]))
        elif temp[temp[4] == i][11].values[0] == 2 or temp[temp[4] == i][11].values[0] == 3:
            change_count+= 1
            change_lis.append('''ALTER TABLE {}  change `{}` `{}` {}  cascade;'''.format(table_name_all,i.lower(),i.lower(), type_conversion(temp[temp[4] == i][5].values[0] if len(temp[temp[4] == i][5]) !=0 else print("Error: table:{} column_type is None".format(table_name_all)), type_dict, column_source,target)))
        else:
            pass
    add_lis = ",".join(add_lis)
    change_lis = "\n".join(change_lis)

    alter_add = '''ALTER TABLE {table_name_all} add columns ({add_lis})'''.format(table_name_all=table_name_all,add_lis=add_lis)
    sql=""
    if add_count!=0 and change_count!=0 :
        sql = "use rds;{alter_add}{change_lis}".format(alter_add=alter_add+';\n',change_lis= change_lis)
    elif add_count!=0 and change_count==0 :
        sql = "use rds;{alter_add}".format(alter_add=alter_add+';\n' )
        print("WARN:dbs:rds_{} table:{} chang is None".format(dbs,table_name_inial))
    elif add_count==0 and change_count!=0 :
        sql = "use rds;{change_lis}".format(change_lis= change_lis)
        print("WARN:dbs:rds_{} table:{} add is None".format(dbs,table_name_inial))
    else :
        print("WARN:dbs:rds_{} table:{} chang is None".format(dbs,table_name_inial))
        print("WARN:dbs:rds_{} table:{} add is None".format(dbs,table_name_inial))
    # print(sql)
    return sql


if __name__ == '__main__':
    pass
