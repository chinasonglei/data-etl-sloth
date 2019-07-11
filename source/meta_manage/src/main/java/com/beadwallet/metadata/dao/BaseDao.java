package com.beadwallet.metadata.dao;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

/**
 * @ClassName BaseDao
 * @Description
 * @Author kai wu
 * @Date 2019/1/18 11:28
 * @Version 1.0
 */
public interface BaseDao {
    /**
     * @Author  kai wu
     * @Description execute update sql
     * @Date  2019/1/18 11:30
     * @Param [connection, sql]
     * @return boolean
     **/
 boolean execute(Connection connection, String sql);

    /**
     * @Author  kai wu
     * @Description execute query sql
     * @Date  2019/1/18 11:30
     * @Param [connection, sql]
     * @return List
     **/
    List<Map<String,Object>> executeQuery(Connection connection, String sql);

}
