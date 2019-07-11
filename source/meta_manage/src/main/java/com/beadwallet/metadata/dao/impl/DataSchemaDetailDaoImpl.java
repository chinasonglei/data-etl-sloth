package com.beadwallet.metadata.dao.impl;

import com.beadwallet.common.entity.DataSchemaDetailEntity;
import com.beadwallet.common.utils.util.CommonUtil;
import com.beadwallet.common.utils.datasourceutil.DataSourceUtil;
import com.beadwallet.metadata.dao.DataSchemaDetailDao;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Repository;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/**
 * @ClassName DataSchemaDetailDaoImpl
 * @Description
 * @Author kai wu
 * @Date 2019/1/16 13:25
 * @Version 1.0
 */
@Repository
public class DataSchemaDetailDaoImpl implements DataSchemaDetailDao {

    Logger logger = LoggerFactory.getLogger(DataSchemaDetailDaoImpl.class);

    /**
     * @param connection
     * @param database
     * @return java.util.List<com.beadwallet.common.entity.DataSchemaDetailEntity>
     * @Author kai wu
     * @Description query List
     * @Date 2019/1/17 17:22
     * @Param [connection, database]
     */
    @Override
    public List<DataSchemaDetailEntity> queryList(Connection connection,String businessSource,String database) throws SQLException{
        //查询结果容器
        List<DataSchemaDetailEntity> list = new ArrayList<DataSchemaDetailEntity>();
        PreparedStatement pst = null;
        ResultSet rs = null;
        //准备查询sql
        StringBuffer sb = new StringBuffer();
        sb.append("select ");
        sb.append("? as business_source,");
        sb.append("table_schema as db_name,");
        sb.append("table_name,");
        sb.append("ordinal_position,");
        sb.append("column_name,");
        //去除类型后的限定关键字
        sb.append("substring_index(column_type,' ',1) as column_type,");
        sb.append("column_comment,");
        sb.append("case when column_key = 'PRI' then 1 else 0 end as column_key,");
        sb.append("case when column_name in ('create_time','time_inst') or column_comment in ('create_time','time_inst') then 1 else 0 end as is_create_time,");
        sb.append("case when column_name in ('update_time','time_upd') or column_comment in ('update_time','time_upd') then 1 else 0 end as is_update_time ");
        sb.append("from information_schema.columns ");
        sb.append("where table_schema = ? ");

        try {
            //进行查询并将结果封装成对象
            pst = connection.prepareStatement(sb.toString());
            pst.setString(1, businessSource);
            pst.setString(2, database);
            rs = pst.executeQuery();

            while (rs.next()) {
                DataSchemaDetailEntity dataSchemaDetailEntity = new DataSchemaDetailEntity();
                dataSchemaDetailEntity.setBusinessSource(rs.getString("business_source"));
                dataSchemaDetailEntity.setDbName(rs.getString("db_name"));
                dataSchemaDetailEntity.setTableName(rs.getString("table_name"));
                dataSchemaDetailEntity.setOrdinalPosition(rs.getInt("ordinal_position"));
                dataSchemaDetailEntity.setColumnName(rs.getString("column_name"));
                dataSchemaDetailEntity.setColumnType(rs.getString("column_type"));
                dataSchemaDetailEntity.setColumnComment(CommonUtil.convertIllegalCharacter(rs.getString("column_comment")));
                dataSchemaDetailEntity.setColumnKey(rs.getInt("column_key"));
                dataSchemaDetailEntity.setIsCreateTime(rs.getInt("is_create_time"));
                dataSchemaDetailEntity.setIsUpdateTime(rs.getInt("is_update_time"));
                dataSchemaDetailEntity.setSensitiveData(0);
                dataSchemaDetailEntity.setUpdateTime(null);
                list.add(dataSchemaDetailEntity);

            }
            logger.info("sql:{} select successfully", sb.toString());
        } catch (SQLException e) {
            e.printStackTrace();
            logger.error("sql:{} select failed,SQLException：{}", sb.toString(),e.getMessage());
            throw new SQLException();
        } finally {
            //关闭流
            DataSourceUtil.closeResultSet(rs);
            DataSourceUtil.closeStatement(pst);
        }
        return list;
    }

    /**
     * @param connection
     * @param list
     * @return int
     * @Author kai wu
     * @Description batch insert
     * @Date 2019/1/17 17:23
     * @Param [connection, list]
     */
    @Override
    public int batchExecuteUpdate(Connection connection, List<DataSchemaDetailEntity> list) throws SQLException{
        int[] rowCounts = {};
        PreparedStatement pst = null;
        String sql = "insert into data_schema_detail_temp(business_source,db_name,table_name,ordinal_position,column_name,column_type,column_comment,column_key,is_create_time,is_update_time,sensitive_data,update_time) values(?,?,?,?,?,?,?,?,?,?,?,?)";
        try {
            pst = connection.prepareStatement(sql);
            Long startTime = System.currentTimeMillis();
            for (int i = 0; i < list.size(); i++) {
                DataSchemaDetailEntity dataSchemaDetailEntity = list.get(i);
                pst.setString(1, dataSchemaDetailEntity.getBusinessSource());
                pst.setString(2, dataSchemaDetailEntity.getDbName());
                pst.setString(3, dataSchemaDetailEntity.getTableName());
                pst.setInt(4, dataSchemaDetailEntity.getOrdinalPosition());
                pst.setString(5, dataSchemaDetailEntity.getColumnName());
                pst.setString(6, dataSchemaDetailEntity.getColumnType());
                pst.setString(7, dataSchemaDetailEntity.getColumnComment());
                pst.setInt(8, dataSchemaDetailEntity.getColumnKey());
                pst.setInt(9, dataSchemaDetailEntity.getIsCreateTime());
                pst.setInt(10, dataSchemaDetailEntity.getIsUpdateTime());
                pst.setInt(11, dataSchemaDetailEntity.getSensitiveData());
                pst.setDate(12, dataSchemaDetailEntity.getUpdateTime());
                pst.addBatch();
            }
            rowCounts = pst.executeBatch();
            Long endTime = System.currentTimeMillis();
            logger.info("Batch insert:{} successfully ,count:{},spend time：{} ms", sql,rowCounts.length, endTime - startTime);
        } catch (SQLException e) {
            e.printStackTrace();
            logger.error("Batch insert:{} failed,SQLException：{}", sql,e.getMessage());
            throw new SQLException();
        } finally {
            //关闭流
            DataSourceUtil.closeStatement(pst);
        }
        return rowCounts.length;
    }



}
