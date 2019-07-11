package com.beadwallet.dao.impl;

import com.beadwallet.cons.Constants;
import com.beadwallet.cons.SQL;
import com.beadwallet.dao.DispatchFlowInfoHistoryDao;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class DispatchFlowInfoHistoryDaoImpl
    extends BaseDao implements DispatchFlowInfoHistoryDao {
    private static Connection connection = null;

    @Override
    public Connection getConnection() {
        if (connection != null) {
            return connection;
        }
        return getConnection(Constants.JDBC_TAG_METASTORE);
    }

    @Override
    public void close() {
        closeConnection(connection);
    }

    @Override
    public boolean insertHistory() {
        Connection connection = getConnection();
        if (connection == null) {
            return false;
        }

        PreparedStatement ps = null;
        try {
            ps = connection.prepareStatement(SQL.DISPATCH_FLOW_INFO_HISTORY_INSERT);
            ps.execute();
            return true;
        } catch (SQLException e) {
            e.printStackTrace();
            try {
                connection.rollback();
            } catch (SQLException e1) {
                e1.printStackTrace();
            }
            return false;
        } finally {
            if (ps != null) {
                try {
                    ps.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }
}