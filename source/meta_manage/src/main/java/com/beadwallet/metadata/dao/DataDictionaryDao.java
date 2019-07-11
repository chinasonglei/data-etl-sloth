package com.beadwallet.metadata.dao;

import com.beadwallet.common.entity.DataDictionaryEntity;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

/**
 * @ClassName DataDictionaryDao
 * @Description
 * @Author kai wu
 * @Date 2019/1/16 13:15
 * @Version 1.0
 */
public interface DataDictionaryDao {

    /**
     * @Author  kai wu
     * @Description query List
     * @Date  2019/1/16 17:14
     * @Param [connection, database]
     * @return java.util.List<com.beadwallet.common.entity.DataDictionaryEntity>
     **/
    List<DataDictionaryEntity> queryList(Connection connection, String businessSource, String dbSource, String dbName) throws SQLException;
    


    /**
     * @Author  kai wu
     * @Description batch insert
     * @Date  2019/1/16 13:20
     * @Param [connection,list]
     * @return int
     **/
    int batchExecuteUpdate(Connection connection,List<DataDictionaryEntity> list) throws SQLException;
}
