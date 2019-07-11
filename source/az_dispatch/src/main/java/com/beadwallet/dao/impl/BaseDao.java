package com.beadwallet.dao.impl;

import com.beadwallet.cons.Constants;
import com.beadwallet.utils.config.ConfigUtil;
import com.beadwallet.utils.jdbc.JDBCConnectionFactory;
import com.beadwallet.utils.jdbc.JDBCConnectionFactoryImpl;
import com.beadwallet.utils.jdbc.JDBCConnectionPojo;
import com.beadwallet.utils.xml.XMLReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;

public class BaseDao {
    private static Logger logger = LoggerFactory.getLogger(BaseDao.class);

    protected Connection getConnection(String tagName) {
        JDBCConnectionPojo jdbcConnectionPojo= (JDBCConnectionPojo) XMLReader.getXMInfo(
                Constants.JDBC_CLASS_NAME,
                ConfigUtil.getProperties(ConfigUtil.CONFIG_PATH),
                tagName,
                1);
        if (jdbcConnectionPojo == null) {
            logger.error("xml reader error");
            return null;
        }

        JDBCConnectionFactory jdbcConnectionFactory = new JDBCConnectionFactoryImpl();
        return jdbcConnectionFactory.getConnection(jdbcConnectionPojo);
    }

    void closeConnection(Connection conn) {
        if (conn != null) {
            try {
                conn.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }
}