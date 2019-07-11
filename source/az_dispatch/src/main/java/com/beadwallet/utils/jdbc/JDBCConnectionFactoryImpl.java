package com.beadwallet.utils.jdbc;

import java.sql.Connection;
import java.sql.DriverManager;

/**
 * JDBC连接实现类
 *
 * @author QuChunhui 2019/01/28
 */
public class JDBCConnectionFactoryImpl implements JDBCConnectionFactory {
    private Connection conn = null;

    public Connection getConnection(JDBCConnectionPojo jdbcConnectionPojo) {
        try {
            Class.forName(jdbcConnectionPojo.getDriver());
            conn = DriverManager.getConnection(
                    jdbcConnectionPojo.getUrl(),
                    jdbcConnectionPojo.getUser(),
                    jdbcConnectionPojo.getPasswd());
        } catch (Exception e) {
            e.printStackTrace();
        }

        return conn;
    }
}