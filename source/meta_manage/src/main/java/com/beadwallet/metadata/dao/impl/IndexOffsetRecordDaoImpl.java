package com.beadwallet.metadata.dao.impl;

import com.beadwallet.common.entity.IndexOffsetRecord;
import com.beadwallet.common.utils.datasourceutil.DataSourceUtil;
import com.beadwallet.metadata.dao.IndexOffsetRecordDao;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Async;
import org.springframework.scheduling.annotation.AsyncResult;
import org.springframework.stereotype.Repository;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Future;

/**
 * @ClassName IndexOffsetRecordDaoImpl
 * @Description
 * @Author kai wu
 * @Date 2019/3/22 12:55
 * @Version 1.0
 */
@Repository
public class IndexOffsetRecordDaoImpl implements IndexOffsetRecordDao {

    Logger logger = LoggerFactory.getLogger(IndexOffsetRecordDaoImpl.class);

    /**
     * 查询List<IndexOffsetRecord>
     *
     * @param connection
     * @param sql
     * @return List<IndexOffsetRecord>
     * @Date 2019/3/21 18:16
     * @Param Connection connection
     * @Param businessSource business source
     */
    @Override
    public List<IndexOffsetRecord> queryList(Connection connection, String sql) {
        //查询结果容器
        List<IndexOffsetRecord> list=new ArrayList<IndexOffsetRecord>();
        PreparedStatement pst = null;
        ResultSet rs = null;
        try{
            pst=connection.prepareStatement(sql);
            rs=pst.executeQuery();
            while(rs.next()){
                IndexOffsetRecord indexOffsetRecord=new IndexOffsetRecord();
                indexOffsetRecord.setId(rs.getInt("id"));
                indexOffsetRecord.setBusinessSource(rs.getString("business_source"));
                indexOffsetRecord.setDbName(rs.getString("db_name"));
                indexOffsetRecord.setTableName(rs.getString("table_name"));
                indexOffsetRecord.setColumnKey(rs.getString("column_name"));
                list.add(indexOffsetRecord);
            }
            logger.info("sql:{} select successfully",sql);

        }catch (SQLException e){
            e.printStackTrace();
            logger.error("sql:{} select failed,SQLException：{}", sql,e.getMessage());
        }finally {
            DataSourceUtil.closeResultSet(rs);
            DataSourceUtil.closeStatement(pst);
        }

        return list;
    }

    /**
     * 批量插入IndexOffsetRecord
     *
     * @param connection
     * @param list
     * @return int
     * @Date 2019/3/21 18:16
     * @Param Connection connection
     * @Param List<IndexOffsetRecord> list
     */
    @Override
    public int batchExecuteUpdate(Connection connection, List<IndexOffsetRecord> list) {
        int[] rowCounts = {};
        PreparedStatement pst = null;
        String sql = "insert into index_offset_records (id,offset,insert_time) values (?,?,?)";
        try {
            pst = connection.prepareStatement(sql);
            Long startTime = System.currentTimeMillis();
            for (int i = 0; i < list.size(); i++) {
                IndexOffsetRecord indexOffsetRecord = list.get(i);
                pst.setInt(1,indexOffsetRecord.getId());
                pst.setLong(2,indexOffsetRecord.getOffSet());
                pst.setString(3,indexOffsetRecord.getInsertTime());
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


    /**
     * 从hive进行查询
     *
     * @param hive
     * @param sql
     * @return Future<IndexOffsetRecord>
     * @Date 2019/3/21 18:16
     * @Param Connection connection
     * @Param String sql
     */
    @Override
    @Async(value = "asyncServiceExecutor")
    public Future<IndexOffsetRecord> queryFromHiveAndInsertToMeta(Connection hive, Connection meta, String sql) {
        PreparedStatement metaPst = null;
        PreparedStatement pst = null;
        ResultSet rs = null;
        IndexOffsetRecord indexOffsetRecord = new IndexOffsetRecord();
        try {
            //1.从数据源查询增量数据
            pst=hive.prepareStatement(sql);
            rs=pst.executeQuery();
            while(rs.next()){
                indexOffsetRecord.setId(rs.getInt("id"));
                indexOffsetRecord.setOffSet(rs.getLong("offset"));
                indexOffsetRecord.setInsertTime(rs.getString("insert_time"));
            }

            //2.将查询结果插入meta
            String insertSql = "insert into index_offset_records(id,offset,insert_time) values(?,?,?)";
            metaPst = meta.prepareStatement(insertSql);
            metaPst.setInt(1,indexOffsetRecord.getId());
            metaPst.setLong(2,indexOffsetRecord.getOffSet());
            metaPst.setString(3,indexOffsetRecord.getInsertTime());
            metaPst.execute();

        } catch (SQLException e) {
            e.printStackTrace();
            logger.error("sql:{} execute failed,SQLException：{}", sql,e.getMessage());
        }finally {
            //关闭流
            DataSourceUtil.closeResultSet(rs);
            DataSourceUtil.closeStatement(pst);
        }

        return new AsyncResult<IndexOffsetRecord>(indexOffsetRecord);
    }
}
