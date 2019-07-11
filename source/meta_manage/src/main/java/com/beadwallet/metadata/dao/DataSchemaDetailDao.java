package com.beadwallet.metadata.dao;

import com.beadwallet.common.entity.DataSchemaDetailEntity;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

/**
 * @ClassName DataSchemaDetailDao
 * @Description
 * @Author kai wu
 * @Date 2019/1/16 13:16
 * @Version 1.0
 */
public interface DataSchemaDetailDao {
    /**
     * @Author  kai wu
     * @Description query List
     * @Date  2019/1/17 17:22
     * @Param [connection, database]
     * @return java.util.List<com.beadwallet.common.entity.DataSchemaDetailEntity>
     **/
    List<DataSchemaDetailEntity> queryList(Connection connection,String businessSource,String database) throws SQLException;


    /**
     * @Author  kai wu
     * @Description batch insert
     * @Date  2019/1/17 17:23
     * @Param [connection, list]
     * @return int
     **/
    int batchExecuteUpdate(Connection connection,List<DataSchemaDetailEntity> list) throws SQLException;

}
