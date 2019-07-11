package com.smallelephant.common.jdbcutil;


import com.smallelephant.common.entity.JDBCConnectionPojo;

import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.Date;


public class DataSourceUtil {


    public static Connection getConnection(JDBCConnectionPojo JDBCConnectionPojo) {
        SimpleDateFormat logSdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Connection connection = null;
        if (JDBCConnectionPojo != null) {
            try {
                Class.forName(JDBCConnectionPojo.getDriver());
                System.out.println(logSdf.format(new Date()) + " DataManageMonitor " + "INFO " + DataSourceUtil.class + " Driver creation successfully!");
                connection = DriverManager.getConnection(
                        JDBCConnectionPojo.getUrl(),
                        JDBCConnectionPojo.getUser(),
                        JDBCConnectionPojo.getPasswd()
                );
                System.out.println(logSdf.format(new Date()) + " DataManageMonitor " + "INFO " + DataSourceUtil.class + " Connection creation successfully!");
            } catch (ClassNotFoundException e) {
                System.out.println(logSdf.format(new Date()) + " DataManageMonitor " + "ERROR " + DataSourceUtil.class + "ClassNotFoundException" + e.getMessage());
            } catch (SQLException e) {
                System.out.println(logSdf.format(new Date())+ " DataManageMonitor " + "ERROR " + DataSourceUtil.class + "SQLException" + e.getMessage());
            }
        } else {
            System.out.println(logSdf.format(new Date()) + " DataManageMonitor " + "ERROR " + DataSourceUtil.class + " Connection creation failed : JdbcConnectionEntity cannot be null!");
        }

        return connection;
    }



    public static void closeAll(ResultSet resultSet, Statement statement, Connection connection) {
        SimpleDateFormat logSdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        try {
            if (resultSet != null) {
                resultSet.close();
            }
            if (statement != null) {
                statement.close();
            }
            if (connection != null) {
                connection.close();
            }
            System.out.println(logSdf.format(new Date()) + " DataManageMonitor " + "INFO " + DataSourceUtil.class + " Connection链接关闭成功");
        } catch (SQLException e) {
            System.out.println(logSdf.format(new Date()) + " DataManageMonitor " + "ERROR " + DataSourceUtil.class + " Connection链接关闭失败 " + e.getMessage());
        }
    }

}
