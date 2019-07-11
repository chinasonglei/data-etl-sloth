package com.beadwallet.dao.impl;

import com.beadwallet.cons.Constants;
import com.beadwallet.cons.SQL;
import com.beadwallet.dao.CronDictionaryDao;
import com.beadwallet.dao.entity.CronDictionaryEntity;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class CronDictionaryDaoImpl extends BaseDao implements CronDictionaryDao {
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
    public List<CronDictionaryEntity> selectCronDicInfo() {
        Connection connection = getConnection();
        if (connection == null) {
            return null;
        }

        PreparedStatement pstmt = null;
        ResultSet rs = null;

        List<CronDictionaryEntity> entityList = new ArrayList<>();
        try {
            pstmt = connection.prepareStatement(SQL.CRON_DICTIONARY_SELECT);
            rs = pstmt.executeQuery();

            while(rs.next()) {
                CronDictionaryEntity entity = new CronDictionaryEntity();
                entity.setId(rs.getInt("id"));
                entity.setMin(rs.getInt("min"));
                entity.setHours(rs.getInt("hours"));
                entity.setDay_of_month(rs.getString("day_of_month"));
                entity.setMonth(rs.getString("month"));
                entity.setDay_of_week(rs.getString("day_of_week"));
                entity.setDelete(rs.getBoolean("delete"));
                entity.setCron_str(toCronStr(entity));
                entityList.add(entity);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            if (rs != null) {
                try {
                    rs.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            if (pstmt != null) {
                try {
                    pstmt.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }

        return entityList;
    }

    private String toCronStr(CronDictionaryEntity entity) {
        //例如：0 0 1 ? 1-12 1-7
        return String.format("0 %s %s %s %s %s",
                entity.getMin(),
                entity.getHours(),
                entity.getDay_of_month(),
                entity.getMonth(),
                entity.getDay_of_week());
    }
}
