package com.beadwallet.metadata.dao.impl;

import com.beadwallet.common.entity.DataDictionaryEntity;
import com.beadwallet.common.utils.datasourceutil.DataSourceUtil;
import com.beadwallet.common.utils.util.CommonUtil;
import com.beadwallet.metadata.dao.DataDictionaryDao;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Repository;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * @ClassName DataDictionaryDaoImpl
 * @Description
 * @Author kai wu
 * @Date 2019/1/16 13:23
 * @Version 1.0
 */
@Repository
public class DataDictionaryDaoImpl implements DataDictionaryDao {

    Logger logger = LoggerFactory.getLogger(DataDictionaryDaoImpl.class);

    /**
     * @Author  kai wu
     * @Description queryList
     * @Date  2019/1/16 16:41
     * @Param [connection, database]
     * @return java.util.List<com.beadwallet.common.entity.DataDictionaryEntity>
     **/
    @Override
    public List<DataDictionaryEntity> queryList(Connection connection, String businessSource, String dbSource, String dbName) throws SQLException{
        //查询结果容器
        List<DataDictionaryEntity> list=new ArrayList<DataDictionaryEntity>();
        PreparedStatement pst = null;
        ResultSet rs = null;
        //准备查询sql
        StringBuffer sb = new StringBuffer();
        sb.append("select ");
        sb.append(" ? as business_source,");
        sb.append(" ? as db_source,");
        sb.append("table_schema as db_name,");
        sb.append("table_name,");
        sb.append("data_length,");
        sb.append("table_comment ");
        sb.append("from information_schema.tables ");
        sb.append("where table_schema = ? ");

        try {
            //进行查询并将结果封装成对象
            pst=connection.prepareStatement(sb.toString());
            pst.setString(1,businessSource);
            pst.setString(2,dbSource);
            pst.setString(3,dbName);
            rs=pst.executeQuery();
            while(rs.next()){
                DataDictionaryEntity dataDictionaryEntity=new DataDictionaryEntity();
                dataDictionaryEntity.setBusinessSource(rs.getString("business_source"));
                dataDictionaryEntity.setDbSource(rs.getString("db_source"));
                dataDictionaryEntity.setDbName(rs.getString("db_name"));
                dataDictionaryEntity.setTableName(rs.getString("table_name"));
                dataDictionaryEntity.setDataLength(rs.getLong("data_length"));
                dataDictionaryEntity.setTableComment(CommonUtil.convertIllegalCharacter(rs.getString("table_comment")));
                dataDictionaryEntity.setTimeOffset(0);
                dataDictionaryEntity.setLastUpdate(null);
                dataDictionaryEntity.setCurrentDdl(0);
                dataDictionaryEntity.setLoad2hive(1);
                dataDictionaryEntity.setDelete(0);
                list.add(dataDictionaryEntity);
            }
            logger.info("sql:{} select successfully", sb.toString());
        } catch (SQLException e) {
            e.printStackTrace();
            logger.error("sql:{} select failed,SQLException：{}", sb.toString(),e.getMessage());
        }finally {
            //关闭流
            DataSourceUtil.closeResultSet(rs);
            DataSourceUtil.closeStatement(pst);
        }
        return list;
    }


    /**
     * @Author  kai wu
     * @Description batch insert
     * @Date  2019/1/16 13:20
     * @Param [connection,list]
     * @return int
     **/
    @Override
    public int batchExecuteUpdate(Connection connection, List<DataDictionaryEntity> list) throws SQLException{
        int[] rowCounts = {};
        PreparedStatement pst = null;
        String sql = "insert into data_dictionary_temp(business_source,db_source,db_name,table_name,data_length,table_comment,time_offset,last_update,load2hive,current_ddl,`delete`) values(?,?,?,?,?,?,?,?,?,?,?)";
        try {
            pst = connection.prepareStatement(sql);
            Long startTime = System.currentTimeMillis();
            for (int i = 0; i < list.size(); i++) {
                DataDictionaryEntity dataDictionaryEntity = list.get(i);
                pst.setString(1,dataDictionaryEntity.getBusinessSource());
                pst.setString(2,dataDictionaryEntity.getDbSource());
                pst.setString(3,dataDictionaryEntity.getDbName());
                pst.setString(4,dataDictionaryEntity.getTableName());
                pst.setLong(5,dataDictionaryEntity.getDataLength());
                pst.setString(6,dataDictionaryEntity.getTableComment());
                pst.setInt(7,dataDictionaryEntity.getTimeOffset());
                pst.setDate(8,dataDictionaryEntity.getLastUpdate());
                pst.setInt(9,dataDictionaryEntity.getLoad2hive());
                pst.setInt(10,dataDictionaryEntity.getCurrentDdl());
                pst.setInt(11,dataDictionaryEntity.getDelete());
                pst.addBatch();
            }
            rowCounts = pst.executeBatch();
            Long endTime = System.currentTimeMillis();
            logger.info("Batch insert:{} successfully ,count:{},spend time：{} ms", sql,rowCounts.length, endTime - startTime);
        } catch (SQLException e) {
            e.printStackTrace();
            logger.error("Batch insert:{} failed,SQLException：{}", sql,e.getMessage());
        }finally {
            //关闭流
            DataSourceUtil.closeStatement(pst);
        }
        return rowCounts.length;
    }
}
