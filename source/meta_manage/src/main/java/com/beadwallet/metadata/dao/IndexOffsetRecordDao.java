package com.beadwallet.metadata.dao;

import com.beadwallet.common.entity.IndexOffsetRecord;

import java.sql.Connection;
import java.util.List;
import java.util.concurrent.Future;

/**
 * @ClassName IndexOffsetRecordDao
 * @Description
 * @Author kai wu
 * @Date 2019/3/22 12:51
 * @Version 1.0
 */
public interface IndexOffsetRecordDao {

    /**
     * 查询List<IndexOffsetRecord>
     *
     * @return List<IndexOffsetRecord>
     * @Date 2019/3/21 18:16
     * @Param Connection connection
     * @Param businessSource business source
     **/
    List<IndexOffsetRecord> queryList(Connection connection, String sql);


    /**
     *  批量插入IndexOffsetRecord
     *
     * @return int
     * @Date 2019/3/21 18:16
     * @Param Connection connection
     * @Param List<IndexOffsetRecord> list
     **/
    int batchExecuteUpdate(Connection connection,List<IndexOffsetRecord> list);

    /**
     *  从hive进行查询
     *
     * @return Future<IndexOffsetRecord>
     * @Date 2019/3/21 18:16
     * @Param Connection connection
     * @Param String sql
     **/
    Future<IndexOffsetRecord> queryFromHiveAndInsertToMeta(Connection hive,Connection meta,String sql);

}
