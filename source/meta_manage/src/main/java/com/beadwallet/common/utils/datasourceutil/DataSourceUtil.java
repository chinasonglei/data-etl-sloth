package com.beadwallet.common.utils.datasourceutil;

import com.beadwallet.common.entity.JdbcConnectionEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;

/**
 * @ClassName DataSourceUtil
 * @Description 操作数据源
 * @Author kai wu
 * @Date 2019/1/11 10:04
 * @Version 1.0
 */
public class DataSourceUtil {

    static Logger logger = LoggerFactory.getLogger(DataSourceUtil.class);

    /**
     * @return java.sql.Connection
     * @Author kai wu
     * @Description Connection获取
     * @Date 2019/1/16 11:01
     * @Param [JdbcConnectionEntity]
     **/
    public static Connection getConnection(JdbcConnectionEntity jdbcConnectionEntity) {
        Connection connection = null;
        if (jdbcConnectionEntity.getDriver() != null) {
            try {
                Class.forName(jdbcConnectionEntity.getDriver());
                logger.info("Driver creation successfully!");
                connection = DriverManager.getConnection(
                        jdbcConnectionEntity.getUrl(),
                        jdbcConnectionEntity.getUser(),
                        jdbcConnectionEntity.getPasswd()
                );
                logger.info("Connection creation successfully!");
            } catch (ClassNotFoundException e) {
                e.printStackTrace();
                logger.error("ClassNotFoundException：{}!", e.getMessage());
            } catch (SQLException e) {
                e.printStackTrace();
                logger.error("SQLException：{}!", e.getMessage());
            }
        } else {
            logger.error("Connection creation failed : driver cannot be null!");
        }

        return connection;
    }



    /**
     * @return void
     * @Author kai wu
     * @Description 关闭流
     * @Date 2019/1/16 11:27
     * @Param []
     **/
    public static void closeAll(ResultSet resultSet, Statement statement, Connection connection) {
        try {
            if(resultSet!=null){
                resultSet.close();
            }
            if(statement!=null){
                statement.close();
            }
            if(connection!=null){
                connection.close();
            }
            logger.info("Close connection!");
        } catch (SQLException e) {
            e.printStackTrace();
            logger.error("SQLException：{}!", e.getMessage());
        }
    }

    /**
     * @Author  kai wu
     * @Description close ResultSet
     * @Date  2019/1/18 11:58
     * @Param [resultSet]
     * @return void
     **/
    public static void closeResultSet(ResultSet resultSet) {
        try {
            if(resultSet!=null){
                resultSet.close();
            }
            logger.info("Close resultSet!");
        } catch (SQLException e) {
            e.printStackTrace();
            logger.error("SQLException：{}!", e.getMessage());
        }
    }

    /**
     * @Author  kai wu
     * @Description close Statement
     * @Date  2019/1/18 11:58
     * @Param [statement]
     * @return void
     **/
    public static void closeStatement(Statement statement) {
        try {
            if(statement!=null){
                statement.close();
            }
            logger.info("Close statement!");
        } catch (SQLException e) {
            e.printStackTrace();
            logger.error("SQLException：{}!", e.getMessage());
        }
    }

    /**
     * @Author  kai wu
     * @Description close Connection
     * @Date  2019/1/18 11:58
     * @Param [connection]
     * @return void
     **/
    public static void closeConnection(Connection connection) {
        try {
            if(connection!=null){
                connection.close();
            }
            logger.info("Close connection!");
        } catch (SQLException e) {
            e.printStackTrace();
            logger.error("SQLException：{}!", e.getMessage());
        }
    }

    /**
     * @Author  kai wu
     * @Description 事务回滚
     * @Date  2019/1/17 11:55
     * @Param [connection]
     * @return void
     **/
    public static void rollback(Connection connection){
        try {
            if(!connection.isClosed()){
                connection.rollback();
                logger.info("rollback!");
            }
        } catch (SQLException e) {
            e.printStackTrace();
            logger.error("SQLException：{}!", e.getMessage());
        }
    }


    /**
     * @Author  kai wu
     * @Description 事务开闭
     * @Date  2019/1/17 11:55
     * @Param [connection]
     * @return void
     **/
    public static void setAutoCommit(Connection connection,boolean status){
        try {
            if(!connection.isClosed()){
                connection.setAutoCommit(status);
                logger.info("setAutoCommit:{}",status);
            }
        } catch (SQLException e) {
            e.printStackTrace();
            logger.error("SQLException：{}!", e.getMessage());
        }
    }
}
