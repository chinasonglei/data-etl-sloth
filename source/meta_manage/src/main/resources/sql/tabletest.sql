create table data_dictionary(
id int AUTO_INCREMENT PRIMARY KEY COMMENT 'PrimaryKey',
business_source varchar(200) COMMENT 'BusinessName',
db_source varchar(200) COMMENT 'DBName',
db_name varchar(200) COMMENT 'DatabaseName',
table_name varchar(200) COMMENT 'TableName',
data_length bigint(100) default -1 COMMENT 'TotalRecords',
table_comment text COMMENT 'Comment',
time_offset int COMMENT 'time_offset',
last_update date COMMENT 'MarkByCode',
current_ddl tinyint(1) default 0 COMMENT 'MarkByCode',
load2hive tinyint(1) default 1 COMMENT 'MarkByArtificial',
`delete` tinyint(1) default 0 COMMENT 'Delete'
)ENGINE=InnoDB DEFAULT CHARSET=utf8;

create table data_schema_detail(
id int COMMENT 'ForeignKey',
db_name varchar(200) COMMENT 'DatabaseName',
table_name varchar(200) COMMENT 'TableName',
ordinal_position int COMMENT 'ColumnPosition',
column_name varchar(200) COMMENT 'Columns',
column_type varchar(200) COMMENT 'DataType',
column_key tinyint(1) default 0 COMMENT 'Mark',
is_create_time tinyint(1) default 0 COMMENT 'Mark',
is_update_time tinyint(1) default 0 COMMENT 'Mark',
column_comment varchar(200) COMMENT 'Mark',
sensitive_data int COMMENT 'Mark',
update_time date COMMENT 'UpdateDate'
)ENGINE=InnoDB DEFAULT CHARSET=utf8;

create table data_update_records(
id int COMMENT 'ForeignKey',
db_name varchar(200) COMMENT 'DatabaseName',
table_name varchar(200) COMMENT 'TableName',
increase_num bigInt(100) default -1 COMMENT 'IncreateNum',
export_num bigInt(100) default -1 COMMENT 'ExportNum',
update_status tinyint(1) default 0 COMMENT 'UpdateStatus',
update_date date COMMENT 'UpdateDate'
)ENGINE=InnoDB DEFAULT CHARSET=utf8;

create database IF NOT EXISTS beadwalletloan charset utf8;
create database IF NOT EXISTS sassevenwallet charset utf8;

drop table data_dictionary;
drop table data_schema_detail;
drop table data_update_records;

alter table data_dictionary add `delete` tinyint(0) COMMENT 'Delete';
alter table data_update_records add `table` tinyint(0) COMMENT 'Delete';


alter table data_schema_detail drop `delete`;
alter table data_schema_detail add foreign key(id) references data_dictionary(id);
alter table data_update_records change increate_num increase_num tinyint() default 0 COMMENT 'IncreateNum';
alter table data_dictionary change current_ddl current_ddl  tinyint() default 0 COMMENT 'MarkByCode';
alter table data_dictionary change load2hive load2hive  tinyint() default 0 COMMENT 'MarkByArtificial';

alter table data_schema_detail add primary key(id);




use beadwalletloan;
create table data_records(
id int AUTO_INCREMENT PRIMARY KEY COMMENT 'PrimaryKey',
db_name varchar(200) COMMENT 'DatabaseName',
table_name varchar(200) COMMENT 'TableName',
increase_num tinyint(1) default 0 COMMENT 'IncreateNum',
export_num tinyint(1) default 0 COMMENT 'ExportNum',
update_status tinyint(1) default 0 COMMENT 'UpdateStatus',
update_date date COMMENT 'UpdateDate',
create_time date COMMENT 'CreateTime',
update_time date COMMENT 'UpdateTime'
);
create table update_thbead_threcords(
id int AUTO_INCREMENT PRIMARY KEY COMMENT 'PrimaryKey',
db_name varchar(200) COMMENT 'DatabaseName',
table_name varchar(200) COMMENT 'TableName',
increase_num tinyint(1) default 0 COMMENT 'IncreateNum',
export_num tinyint(1) default 0 COMMENT 'ExportNum',
update_status tinyint(1) default 0 COMMENT 'UpdateStatus',
update_date date COMMENT 'UpdateDate',
create_time date COMMENT 'CreateTime',
update_time date COMMENT 'UpdateTime'
);
drop table data_records;
drop table update_records;
INSERT INTO update_rebead_rerecords(db_name,table_name,increase_num,export_num,update_status,update_date,create_time,update_time)
VALUES ('beadwalletloan','update_rebead_rerecords',0,0,0,'2019-02-01','2019-02-01','2019-02-01');


use sassevenwallet;
create table data_scheme(
id int AUTO_INCREMENT PRIMARY KEY COMMENT 'PrimaryKey',
db_name varchar(200) COMMENT 'DatabaseName',
table_name varchar(200) COMMENT 'TableName',
increase_num tinyint(1) default 0 COMMENT 'IncreateNum',
export_num tinyint(1) default 0 COMMENT 'ExportNum',
update_status tinyint(1) default 0 COMMENT 'UpdateStatus',
update_date date COMMENT 'UpdateDate',
create_time date COMMENT 'CreateTime',
update_time date COMMENT 'UpdateTime'
);
create table update_detail(
id int AUTO_INCREMENT PRIMARY KEY COMMENT 'PrimaryKey',
db_name varchar(200) COMMENT 'DatabaseName',
table_name varchar(200) COMMENT 'TableName',
increase_num tinyint(1) default 0 COMMENT 'IncreateNum',
export_num tinyint(1) default 0 COMMENT 'ExportNum',
update_status tinyint(1) default 0 COMMENT 'UpdateStatus',
update_date date COMMENT 'UpdateDate',
create_time date COMMENT 'CreateTime',
update_time date COMMENT 'UpdateTime'
);

create table update_thsass_rereport(
id int AUTO_INCREMENT PRIMARY KEY COMMENT 'PrimaryKey',
db_name varchar(200) COMMENT 'DatabaseName',
table_name varchar(200) COMMENT 'TableName',
increase_num int COMMENT 'IncreateNum',
export_num int COMMENT 'ExportNum',
update_status tinyint(1) default 0 COMMENT 'UpdateStatus',
update_date date COMMENT 'UpdateDate',
create_time date COMMENT 'CreateTime',
update_time date COMMENT 'UpdateTime'
);

drop table data_scheme;
drop table update_detail;

INSERT INTO update_sass_rereport(db_name,table_name,increase_num,export_num,update_status,update_date,create_time,update_time)
VALUES ('sassevenwallet','update_sass_rereport',0,0,0,'2019-02-01','2019-02-01','2019-02-01');



insert into data_update_records(id,db_name,table_name,increase_num,update_date) values(02,'beadwalletloan','testtable',1,'2019-02-01')
																																																			


UPDATE data_dictionary set load2hive=0 
WHERE id IN( 
	SELECT id from(
		SELECT *,(mark1+mark2+mark3) as mark FROM(
			SELECT id,table_name,SUM(column_key) as mark1,SUM(is_create_time) as mark2, 
			SUM(is_update_time) as mark3
		FROM data_schema_detail GROUP BY id ) as temp1
	) as temp2 
where mark<3 