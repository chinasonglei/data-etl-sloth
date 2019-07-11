package com.beadwallet.dao.impl;

import com.beadwallet.cons.Constants;
import com.beadwallet.cons.SQL;
import com.beadwallet.dao.MetaExecuteDao;
import com.beadwallet.dao.entity.MetaExecuteEntity;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class MetaExecuteDaoImpl extends BaseDao implements MetaExecuteDao {
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
    public boolean insert(MetaExecuteEntity entity) {
        if (entity == null) {
            return false;
        }

        Connection connection = getConnection();
        if (connection == null) {
            return false;
        }

        PreparedStatement ps = null;
        try {
            ps = connection.prepareStatement(SQL.META_EXECUTE_INSERT);
            int i = 1;
            ps.setString(i++, entity.getModule());
            ps.setBoolean(i++, entity.isStatus());
            ps.setDate(i++, entity.getDate());
            ps.setString(i++, entity.getModule());
            ps.setBoolean(i++, entity.isStatus());
            ps.setDate(i, entity.getDate());
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