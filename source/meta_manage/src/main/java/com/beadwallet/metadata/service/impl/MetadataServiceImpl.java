package com.beadwallet.metadata.service.impl;

import com.beadwallet.common.entity.*;
import com.beadwallet.common.utils.datasourceutil.DataSourceUtil;
import com.beadwallet.common.utils.xmlutil.XMLReader;
import com.beadwallet.metadata.dao.BaseDao;
import com.beadwallet.metadata.dao.DataDictionaryDao;
import com.beadwallet.metadata.dao.DataSchemaDetailDao;
import com.beadwallet.metadata.service.MetadataService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.sql.Connection;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * @ClassName MetadataServiceImpl
 * @Description
 * @Author kai wu
 * @Date 2019/1/21 13:46
 * @Version 1.0
 */
@Service
public class MetadataServiceImpl implements MetadataService {

    Logger logger = LoggerFactory.getLogger(MetadataServiceImpl.class);


    @Value("${jdbc.connection.entity}")
    private String jdbcEntity;

    @Value("${time.offset.entity}")
    private String timeOffsetEntity;

    @Value("${metastore.type}")
    private String metaStoreType;

    @Value("${jdbc.type}")
    private String jdbcType;

    @Value("${mysql.time.offset}")
    private String timeOffset;

    @Autowired
    private BaseDao baseDao;

    @Autowired
    private DataDictionaryDao dataDictionaryDao;

    @Autowired
    private DataSchemaDetailDao dataSchemaDetailDao;

    /**
     * update metadata
     * @param xmlPath
     * @return boolean
     */
    @Override
    public boolean updateMetadata(String xmlPath) {
        boolean result = false;
        Connection targetConn = null;
        try {
            //解析xml文件，获取数据字典元数据库MySQL配置信息
            ArrayList<JdbcConnectionEntity> metaStore = XMLReader.getXMInfo(jdbcEntity, xmlPath, metaStoreType);
            //解析xml文件，获取业务库MySQL连接配置
            ArrayList<JdbcConnectionEntity> jdbcList = XMLReader.getXMInfo(jdbcEntity, xmlPath, jdbcType);
            //1.获取target connection，并关闭事务自动提交
            JdbcConnectionEntity target = metaStore.get(0);
            targetConn = DataSourceUtil.getConnection(target);
            //2.对当前数据字典进行数据备份
            dropBackupTable(targetConn);
            backup(targetConn);
            //3.创建中间表data_dictionary_temp与data_scheme_detail_temp用于存储从数据源获取的当日数据
            dropTempTable(targetConn);
            createTempTable(targetConn);
            //4.将从数据源获取的当日数据插入中间表
            insertDataToTempTable(xmlPath,jdbcList, targetConn);
            //开启手动事务
            targetConn.setAutoCommit(false);
            //5.关联当前字典与中间表，获取最新的数据字典
            //5.1业务库删除表
            sourceDeleteTable(targetConn);
            //5.2业务库新增表
            sourceAddTable(targetConn);
            //5.3.更新time_offset
            updateTimeOffset(targetConn);
            //更新中间表data_schema_detail_temp的id字段
            addIdForTemp(targetConn);
            //同一张表包含create_time/update_time和time_inst/time_upd，优先取time_inst/time_upd
            deleteDuplicatedTimeColumn(targetConn);
            //5.4业务库删除或修改表字段
            sourceDeleteColumn(targetConn);
            //5.5业务库新增字段
            sourceAddColumn(targetConn);
            //5.6业务库修改字段类型
            updateColumnType(targetConn);
            //5.7业务库修改字段注释
            updateColumnComment(targetConn);
            //5.8业务库删除或新增字段时，同步ordinal_position字段
            updateOrdinalPosition(targetConn);
            //5.9元数据同步完毕后更新主表
            updateMain(targetConn);
            //5.10删除中间表
            dropTempTable(targetConn);
            //5.11排除运营商大表
            undispatchTable(targetConn);
            //5.12查询schema_detail表,将不符合条件的表名和主表id获取到，根据主表id Update data_dictionary表，将load2hive字段修改为0
            updateDictionaryLoad2HiveStatement(targetConn);
            //6.统计完毕后，向流程表插入一条记录，用来标识元数据同步模块执行完毕
            StringBuffer recordDelete = new StringBuffer();
            recordDelete.append("delete from meta_execute_records ");
            baseDao.execute(targetConn,recordDelete.toString());
            StringBuffer recordInsert = new StringBuffer();
            recordInsert.append("insert into meta_execute_records values('meta_manage',1,current_date)");
            baseDao.execute(targetConn,recordInsert.toString());
            //7.手动提交事务
            targetConn.commit();
            result = true;
        } catch (Exception e) {
            logger.error("Exception：{}", e.toString());
            // 若出现异常则回滚到事务开始状态
            DataSourceUtil.rollback(targetConn);
            //mysql数据库DDL操作直接触发隐式提交，事务立即生效，无法回滚，需进行手动更改操作
            dropBackupTable(targetConn);
            dropTempTable(targetConn);
        } finally {
            DataSourceUtil.setAutoCommit(targetConn, true);
            DataSourceUtil.closeConnection(targetConn);
        }

        return result;
    }


    /**
     * drop backup table
     *
     */
    public void dropBackupTable(Connection targetConn) {
        SimpleDateFormat df = new SimpleDateFormat("yyyyMMdd");
        String currentDate = df.format(new Date());
        String dropParent = "drop table if exists data_dictionary_" + currentDate + "";
        String dropSon = "drop table if exists data_schema_detail_" + currentDate + "";
        baseDao.execute(targetConn, dropParent);
        baseDao.execute(targetConn, dropSon);
    }

    /**
     * data backup
     *
     */
    public void backup(Connection targetConn) {
        SimpleDateFormat df = new SimpleDateFormat("yyyyMMdd");
        String currentDate = df.format(new Date());
        String createParent = "create table data_dictionary_" + currentDate + " like data_dictionary";
        String createSon = "create table data_schema_detail_" + currentDate + " like data_schema_detail";
        baseDao.execute(targetConn, createParent);
        baseDao.execute(targetConn, createSon);
        String insertParent = "insert into data_dictionary_" + currentDate + " select * from data_dictionary";
        String insertSon = "insert into data_schema_detail_" + currentDate + " select * from data_schema_detail";
        baseDao.execute(targetConn, insertParent);
        baseDao.execute(targetConn, insertSon);
    }


    /**
     * create temp table for synchronous update metadata
     *
     */
    public void dropTempTable(Connection targetConn) {
        String dropTempParent = "drop table if exists data_dictionary_temp";
        String dropTempSon = "drop table if exists data_schema_detail_temp";
        baseDao.execute(targetConn, dropTempParent);
        baseDao.execute(targetConn, dropTempSon);
    }

    /**
     * undispatchTable
     *
     */
    public void undispatchTable(Connection targetConn) {
        StringBuffer undispatchTable = new StringBuffer();
        undispatchTable.append("update data_dictionary a inner join  ");
        undispatchTable.append("undispatch_table b on a.business_source=b.business_source and a.db_name=b.db_name and a.table_name=b.table_name ");
        undispatchTable.append("set a.load2hive= 0  ");
        baseDao.execute(targetConn, undispatchTable.toString());
    }

    /**
     * update dictionary Load2Hive statement
     *
     */
    public void updateDictionaryLoad2HiveStatement(Connection targetConn) {
        StringBuffer updateDictionaryLoad2HiveStatement = new StringBuffer();
        updateDictionaryLoad2HiveStatement.append("UPDATE data_dictionary set load2hive=0 ");
        updateDictionaryLoad2HiveStatement.append("WHERE id IN(  ");
        updateDictionaryLoad2HiveStatement.append("SELECT id FROM(SELECT id,table_name,SUM(column_key) as mark1,SUM(is_create_time) as mark2,  ");
        updateDictionaryLoad2HiveStatement.append("SUM(is_update_time) as mark3 FROM data_schema_detail GROUP BY id ) as temp1 ");
        updateDictionaryLoad2HiveStatement.append("where mark1 <> 1 or (mark1 = 1 and (mark1+mark2+mark3)<2) ) ");
        baseDao.execute(targetConn, updateDictionaryLoad2HiveStatement.toString());
    }


    /**
     * create temp table for synchronous update metadata
     *
     */
    public void createTempTable(Connection targetConn) {
        String createTempParent = "create table data_dictionary_temp like data_dictionary";
        String createTempSon = "create table data_schema_detail_temp like data_schema_detail";
        String addTempSonColumn = "alter table data_schema_detail_temp add business_source varchar(200)";
        baseDao.execute(targetConn, createTempParent);
        baseDao.execute(targetConn, createTempSon);
        baseDao.execute(targetConn, addTempSonColumn);
    }


    /**
     * get data from source and insert into temp table
     *
     */
    public void insertDataToTempTable(String xmlPath,ArrayList<JdbcConnectionEntity> jdbcList, Connection targetConn) throws SQLException {
        Connection sourceConn = null;
        List<DataDictionaryEntity> dataDictionaryList = new ArrayList<>();
        List<DataSchemaDetailEntity> schemaDetailEntityList = new ArrayList<>();
        for (JdbcConnectionEntity jdbc : jdbcList) {
            sourceConn = DataSourceUtil.getConnection(jdbc);
            dataDictionaryList.addAll(dataDictionaryDao.queryList(sourceConn, jdbc.getBusinessSource(), "MySQL", jdbc.getDbName()));
            schemaDetailEntityList.addAll(dataSchemaDetailDao.queryList(sourceConn,jdbc.getBusinessSource(), jdbc.getDbName()));
            DataSourceUtil.closeConnection(sourceConn);
        }
        ArrayList<TimeOffsetEntity> timeOffsetEntities = XMLReader.getXMInfo(timeOffsetEntity, xmlPath, timeOffset);

        List<DataDictionaryEntity> resultDictionaryEntities = new ArrayList<>();
        for (DataDictionaryEntity dictionaryEntity : dataDictionaryList) {
            String dbName = dictionaryEntity.getDbName();
            String tableName = dictionaryEntity.getTableName();
            for (TimeOffsetEntity timeOffsetEntity : timeOffsetEntities) {
                if (dbName.equals(timeOffsetEntity.getDbName()) && tableName.equals(timeOffsetEntity.getTableName())) {
                    dictionaryEntity.setTimeOffset(Integer.parseInt(timeOffsetEntity.getTimeOffset()));
                }
            }

            resultDictionaryEntities.add(dictionaryEntity);
        }

        dataDictionaryDao.batchExecuteUpdate(targetConn, resultDictionaryEntities);
        dataSchemaDetailDao.batchExecuteUpdate(targetConn, schemaDetailEntityList);
    }

    /**
     * source delete table
     *
     */
    public void sourceDeleteTable(Connection targetConn) {
        StringBuffer sb = new StringBuffer();
        sb.append("update data_dictionary a inner join  ");
        sb.append("(select  ");
        sb.append("dd.id,");
        sb.append("dd.db_name,");
        sb.append("dd.table_name,");
        sb.append("0 as load2hive,");
        sb.append("current_date as last_update ");
        sb.append("from data_dictionary dd   ");
        sb.append("left join data_dictionary_temp ddt on ddt.business_source = dd.business_source and ddt.db_name = dd.db_name and ddt.table_name = dd.table_name ");
        sb.append("where ddt.business_source is null and ddt.db_name is null and ddt.table_name is null ");
        sb.append(")b on a.id = b.id set a.load2hive = b.load2hive and a.last_update = b.last_update ");
        baseDao.execute(targetConn, sb.toString());
    }

    /**
     * source add table
     *
     */
    public void sourceAddTable(Connection targetConn) {
        StringBuffer sb = new StringBuffer();
        sb.append("insert into data_dictionary  ");
        sb.append("(business_source,db_source,db_name,table_name,data_length,table_comment,time_offset,load2hive,`delete`,current_ddl,last_update,storage) ");
        sb.append("select ");
        sb.append("ddt.business_source,");
        sb.append("ddt.db_source,");
        sb.append("ddt.db_name,");
        sb.append("ddt.table_name,");
        sb.append("ddt.data_length,");
        sb.append("ddt.table_comment,");
        sb.append("ddt.time_offset,");
        sb.append("ddt.load2hive,");
        sb.append("ddt.`delete` ,");
        sb.append("1 as current_ddl,");
        sb.append("current_date as last_update, ");
        sb.append("DATE_FORMAT(current_date,'%Y%m%d') as storage ");
        sb.append("from data_dictionary_temp ddt  ");
        sb.append("left join data_dictionary dd on ddt.business_source = dd.business_source and ddt.db_name = dd.db_name and ddt.table_name = dd.table_name ");
        sb.append("where dd.business_source is null and dd.db_name is null and dd.table_name is null  ");
        baseDao.execute(targetConn, sb.toString());
    }

    /**
     * update column time_offset
     *
     */
    public void updateTimeOffset(Connection targetConn) {
        StringBuffer sb = new StringBuffer();
        sb.append("update data_dictionary a inner join ");
        sb.append("data_dictionary_temp b on a.business_source = b.business_source and a.db_name = b.db_name and a.table_name = b.table_name  ");
        sb.append("set a.time_offset = b.time_offset   ");
        baseDao.execute(targetConn, sb.toString());
    }


    /**
     * addIdForTemp
     *
     */
    public void addIdForTemp(Connection targetConn) {
        StringBuffer sb = new StringBuffer();
        sb.append("update data_schema_detail_temp dsdt ");
        sb.append("inner join data_dictionary dd on dd.business_source = dsdt.business_source and dd.db_name = dsdt.db_name and dd.table_name = dsdt.table_name  ");
        sb.append("set dsdt.id = dd.id   ");
        baseDao.execute(targetConn, sb.toString());
    }

    /**
     * addIdForTemp
     *
     */
    public void deleteDuplicatedTimeColumn(Connection targetConn) {
        // 更新创建时间
        StringBuffer sb = new StringBuffer();
        sb.append("update data_schema_detail_temp a inner join ");
        sb.append("(select a.id, ");
        sb.append("a.column_name,");
        sb.append("case when a.column_name = 'time_inst' then 1 else 0 end as is_create_time ");
        sb.append("from data_schema_detail_temp a  ");
        sb.append("inner join (select id from data_schema_detail_temp where is_create_time = 1 group by id having sum(is_create_time) > 1)b on a.id = b.id  ");
        sb.append(")b on a.id = b.id and a.column_name = b.column_name set a.is_create_time = b.is_create_time   ");
        baseDao.execute(targetConn, sb.toString());

        // 更新更新时间
        StringBuffer sb2 = new StringBuffer();
        sb2.append("update data_schema_detail_temp a inner join  ");
        sb2.append("(select a.id, ");
        sb2.append("a.column_name,");
        sb2.append("case when a.column_name = 'time_upd' then 1 else 0 end as is_update_time ");
        sb2.append("from data_schema_detail_temp a  ");
        sb2.append("inner join (select id from data_schema_detail_temp where is_update_time = 1 group by id having sum(is_update_time) > 1)b on a.id = b.id ");
        sb2.append(")b on a.id = b.id and a.column_name = b.column_name set a.is_update_time = b.is_update_time   ");
        baseDao.execute(targetConn, sb2.toString());
    }

    /**
     * source table delete column
     *
     */
    public void sourceDeleteColumn(Connection targetConn) {
        StringBuffer sb1 = new StringBuffer();
        sb1.append("update data_schema_detail a inner join   ");
        sb1.append("(select  ");
        sb1.append("dsd.id,");
        sb1.append("dsd.column_name  ");
        sb1.append("from (select * from data_schema_detail where status is null or status <> 0) dsd   ");
        sb1.append("left join data_schema_detail_temp dsdt on dsd.id = dsdt.id and dsd.column_name = dsdt.column_name   ");
        sb1.append("where dsdt.id is null and dsdt.column_name is null ");
        sb1.append(")b on a.id = b.id and a.column_name = b.column_name set a.status = 0,a.busi_column = 0  ");
        baseDao.execute(targetConn, sb1.toString());
    }

    /**
     * source table add column
     *
     */
    public void sourceAddColumn(Connection targetConn) {
        StringBuffer sb1 = new StringBuffer();
        sb1.append("insert into data_schema_detail  ");
        sb1.append("(id,db_name,table_name,ordinal_position,column_name,column_type,column_comment,column_key,is_create_time,is_update_time,sensitive_data,status,update_time)  ");
        sb1.append("select  ");
        sb1.append("dsdt.id, ");
        sb1.append("dsdt.db_name, ");
        sb1.append("dsdt.table_name, ");
        sb1.append("dsdt.ordinal_position, ");
        sb1.append("dsdt.column_name,");
        sb1.append("dsdt.column_type,");
        sb1.append("dsdt.column_comment,");
        sb1.append("dsdt.column_key,");
        sb1.append("dsdt.is_create_time,");
        sb1.append("dsdt.is_update_time,");
        sb1.append("dsdt.sensitive_data,");
        sb1.append("case when dd.storage = current_date then null else 1 end as status,");
        sb1.append("current_date as update_time ");
        sb1.append("from data_schema_detail_temp dsdt  ");
        sb1.append("inner join data_dictionary dd on dsdt.id = dd.id  ");
        sb1.append("left join data_schema_detail dsd on dsd.id = dsdt.id and dsd.column_name = dsdt.column_name   ");
        sb1.append("where dsd.id is null and dsd.column_name is null  ");
        baseDao.execute(targetConn, sb1.toString());

    }

    /**
     * update columnType
     *
     */
    public void updateColumnType(Connection targetConn) {
        StringBuffer sb1 = new StringBuffer();
        sb1.append("update data_schema_detail a  ");
        sb1.append("inner join data_schema_detail_temp b on a.id = b.id and a.column_name = b.column_name  ");
        sb1.append("set a.column_type = b.column_type ,");
        sb1.append("a.status = (case when substring_index(a.column_type,'(',1) <> substring_index(b.column_type,'(',1) then 2 end),");
        sb1.append("a.update_time = current_date ");
        sb1.append("where substring_index(a.column_type,'(',1) <> substring_index(b.column_type,'(',1)  ");
        baseDao.execute(targetConn, sb1.toString());
    }


    /**
     * update columnComment
     *
     */
    public void updateColumnComment(Connection targetConn) {
        StringBuffer sb1 = new StringBuffer();
        sb1.append("update data_schema_detail a  ");
        sb1.append("inner join data_schema_detail_temp b on a.id = b.id and a.column_name = b.column_name  ");
        sb1.append("set a.column_comment = b.column_comment,");
        sb1.append("a.status = 3,");
        sb1.append("a.update_time = current_date ");
        sb1.append("where a.column_comment <> b.column_comment   ");
        baseDao.execute(targetConn, sb1.toString());
    }


     /**
     * update ordinal position
     *
     */
    public void updateOrdinalPosition(Connection targetConn) {
        StringBuffer sb1 = new StringBuffer();
        sb1.append("update data_schema_detail a  ");
        sb1.append("inner join ( ");
        sb1.append("select  ");
        sb1.append("t.id, ");
        sb1.append("t.column_name, ");
        sb1.append("t.rank as ordinal_position ");
        sb1.append("from (  ");
        sb1.append(" select ");
        sb1.append(" tmp.id,");
        sb1.append(" tmp.column_name, ");
        sb1.append(" tmp.ordinal_position, ");
        sb1.append(" tmp.update_time,");
        sb1.append(" tmp.status, ");
        sb1.append(" if(@id=tmp.id,@rank:=@rank+1,@rank:=1) as rank, ");
        sb1.append(" @id:=tmp.id ");
        sb1.append(" from (  ");
        sb1.append("     select ");
        sb1.append("     a.id, ");
        sb1.append("     a.column_name, ");
        sb1.append("     a.ordinal_position, ");
        sb1.append("     a.update_time, ");
        sb1.append("     a.status  ");
        sb1.append("     from data_schema_detail a   ");
        sb1.append("     inner join   ");
        sb1.append("     (select distinct id from data_schema_detail where status = 1  and update_time = current_date) b  ");
        sb1.append("     on a.id = b.id order by a.id,a.update_time,a.ordinal_position asc  ");
        sb1.append("   ) tmp ,(select @id := null ,@rank:=0) a   ");
        sb1.append("  ) t where t.update_time = current_date and t.status = 1    ");
        sb1.append(") b on a.id = b.id and a.column_name = b.column_name set a.ordinal_position = b.ordinal_position ");
        baseDao.execute(targetConn, sb1.toString());
    }


    /**
     * update updateMain
     *
     */
    public void updateMain(Connection targetConn){
        StringBuffer sb1 = new StringBuffer();
        sb1.append("update data_dictionary a inner join  ");
        sb1.append("(select id from data_schema_detail where update_time = current_date group by id )b  ");
        sb1.append("on a.id = b.id set a.last_update = current_date  ");
        baseDao.execute(targetConn, sb1.toString());
    }

}
