-- 数仓元数据同步逻辑如下：
-- 1.元数据备份，保留历史数据，创建中间表用于元数据的同步
-- 2.业务数据源删除表时，数据系统data_dictionary表，load2hive字段置为0，表示该表后续不会进行etl操作，子表data_schema_detail字段不进行删除操作,status字段置为0，busi_column字段置为0
-- 3.业务数据源新增表时，数据系统元数据模块同步新增
-- 4.业务数据源删除字段未删除表时，数据系统元数据模块data_schema_detail字段不进行删除操作,status字段置为0，busi_column字段置为0
-- 5.业务数据源新增字段时，数据系统元数据模块同步新增,data_schema_detail字段不进行删除操作,status字段置为1

-- 具体操作步骤如下：
-- 1.数仓元数据备份
drop table if exists data_dictionary_20190117;
drop table if exists data_schema_detail_20190117;
create table data_dictionary_20190117 like data_dictionary;
insert into data_dictionary_20190117 select * from data_dictionary;
create table data_schema_detail_20190117 like data_schema_detail;
insert into data_schema_detail_20190117 select * from data_schema_detail;


-- 2.创建中间表data_dictionary_temp与data_scheme_detail_temp用于存储从数据源获取的最新元数据
drop table if exists data_dictionary_temp;
drop table if exists data_schema_detail_temp;
create table data_dictionary_temp like data_dictionary;
create table data_schema_detail_temp like data_schema_detail;
alter table data_schema_detail_temp add business_source varchar(200);


--  如何判断column 是否为is_create_time与is_update_time
-- 3.将从数据源获取的当日数据插入中间表
insert into data_dictionary_temp select xxx,xxx,..... from information_schema.tables where table_schema = ?;
insert into data_schema_detail_temp select xxx,xxx,..... from information_schema.columns where table_schema = ?;


-- 表级操作
-- 4.业务库删除表
-- 更新主表
update data_dictionary a inner join 
(select 
dd.id,
dd.db_name,
dd.table_name,
0 as load2hive,
current_date as last_update 
from data_dictionary dd 
left join data_dictionary_temp ddt on ddt.business_source = dd.business_source and ddt.db_name = dd.db_name and ddt.table_name = dd.table_name 
where ddt.business_source is null and ddt.db_name is null and ddt.table_name is null 
)b on a.id = b.id set a.load2hive = b.load2hive and a.last_update = b.last_update 



--5.业务库新增表
insert into data_dictionary 
(business_source,db_source,db_name,table_name,data_length,table_comment,time_offset,load2hive,`delete`,current_ddl,last_update) 
select cluster-enabled
ddt.business_source,
ddt.db_source,
ddt.db_name,
ddt.table_name,
ddt.data_length,
ddt.table_comment,
ddt.time_offset,
ddt.load2hive,
ddt.`delete`,
1 as current_ddl,
current_date as last_update
from data_dictionary_temp ddt
left join data_dictionary dd on ddt.business_source = dd.business_source and ddt.db_name = dd.db_name and ddt.table_name = dd.table_name
where dd.business_source is null and dd.db_name is null and dd.table_name is null


-- 6.更新time_offset字段
update data_dictionary a inner join
data_dictionary_temp b on a.business_source = b.business_source and a.db_name = b.db_name and a.table_name = b.table_name
set a.time_offset = b.time_offset


-- 7.更新中间表data_schema_detail_temp的id字段
update data_schema_detail_temp dsdt
inner join data_dictionary dd on dd.business_source = dsdt.business_source and dd.db_name = dsdt.db_name and dd.table_name = dsdt.table_name
set dsdt.id = dd.id

-- 因当前对创建和更新时间的判断无统一标识，对同一张表包含create_time/update_time和time_inst/time_upd，优先取time_inst/time_upd
update data_schema_detail_temp a inner join
(select a.id,
a.column_name,
case when a.column_name = 'time_inst' then 1 else 0 end as is_create_time
from data_schema_detail_temp a
inner join (select id from data_schema_detail_temp where is_create_time = 1 group by id having sum(is_create_time) > 1)b on a.id = b.id
)b on a.id = b.id and a.column_name = b.column_name set a.is_create_time = b.is_create_time

update data_schema_detail_temp a inner join
(select a.id,
a.column_name,
case when a.column_name = 'time_upd' then 1 else 0 end as is_update_time
from data_schema_detail_temp a
inner join (select id from data_schema_detail_temp where is_update_time = 1 group by id having sum(is_update_time) > 1)b on a.id = b.id
)b on a.id = b.id and a.column_name = b.column_name set a.is_update_time = b.is_update_time

-- 字段级操作
-- 8.业务库删除表字段(status=0)
update data_schema_detail a inner join
(select
dsd.id,
dsd.column_name
from (select * from data_schema_detail where status is null or status <> 0) dsd
left join data_schema_detail_temp dsdt on dsd.id = dsdt.id and dsd.column_name = dsdt.column_name
where dsdt.id is null and dsdt.column_name is null
)b on a.id = b.id and a.column_name = b.column_name set a.status = 0,a.busi_column = 0


-- 9.业务库新增字段(status=1)
insert into data_schema_detail
(id,db_name,table_name,ordinal_position,column_name,column_type,column_comment,column_key,is_create_time,is_update_time,sensitive_data,status,update_time)
select
dsdt.id,
dsdt.db_name,
dsdt.table_name,
dsdt.ordinal_position,
dsdt.column_name,
dsdt.column_type,
dsdt.column_comment,
dsdt.column_key,
dsdt.is_create_time,
dsdt.is_update_time,
dsdt.sensitive_data,
case when dd.storage = current_date then null else 1 end as status,
current_date as update_time
from data_schema_detail_temp dsdt
inner join data_dictionary dd on dsdt.id = dd.id
left join data_schema_detail dsd on dsd.id = dsdt.id and dsd.column_name = dsdt.column_name
where dsd.id is null and dsd.column_name is null

--10.1 业务库修改字段类型(status=2)
update data_schema_detail a
inner join data_schema_detail_temp b on a.id = b.id and a.column_name = b.column_name
set a.column_type = b.column_type ,
a.status = (case when substring_index(a.column_type,'(',1) <> substring_index(b.column_type,'(',1) then 2 end),
a.update_time = current_date
where substring_index(a.column_type,'(',1) <> substring_index(b.column_type,'(',1)


--10.2 业务库修改字段注释(status=3)
update data_schema_detail a
inner join data_schema_detail_temp b on a.id = b.id and a.column_name = b.column_name
set a.column_comment = b.column_comment,
a.status = 3,
a.update_time = current_date
where a.column_comment <> b.column_comment

--10.3 业务库新增字段时，data_schema_detail同步ordinal_position字段值(逻辑为追加)
update data_schema_detail a 
inner join (
select 
t.id,
t.column_name,
t.rank as ordinal_position 
from (
	select 
	tmp.id,
	tmp.column_name,
	tmp.ordinal_position,
	tmp.update_time,
	tmp.status, 
	if(@id=tmp.id,@rank:=@rank+1,@rank:=1) as rank,  
	@id:=tmp.id  
	from (   
		select 
		a.id,
		a.column_name,
		a.ordinal_position,
		a.update_time,
		a.status 
		from data_schema_detail a 
		inner join 
		(select distinct id from data_schema_detail where status = 1  and update_time = current_date) b   
		on a.id = b.id order by a.id,a.update_time,a.ordinal_position asc   
	) tmp ,(select @id := null ,@rank:=0) a 
) t where t.update_time = current_date and t.status = 1 
) b on a.id = b.id and a.column_name = b.column_name set a.ordinal_position = b.ordinal_position 


-- 11.更新主表
update data_dictionary a inner join
(select id from data_schema_detail where update_time = current_date group by id )b
on a.id = b.id set a.last_update = current_date


-- 12.删除中间表
drop table if exists data_dictionary_temp;
drop table if exists data_schema_detail_temp;



-- 问题汇总
-- 1.当前判断column是否为is_create_time与is_update_time的逻辑逻辑为
column_name in ('create_time','time_inst') or column_comment in ('create_time','time_inst')
column_name in ('update_time','time_upd') or column_comment in ('update_time','time_upd')