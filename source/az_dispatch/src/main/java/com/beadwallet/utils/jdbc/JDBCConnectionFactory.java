package com.beadwallet.utils.jdbc;

import java.sql.Connection;

/**
 * JDBC工厂接口类
 *
 * @author QuChunhui 2019/01/28
 */
public interface JDBCConnectionFactory {
    public Connection getConnection(JDBCConnectionPojo jdbcConnectionPojo);
}
