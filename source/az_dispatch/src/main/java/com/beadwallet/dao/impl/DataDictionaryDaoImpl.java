package com.beadwallet.dao.impl;

import com.beadwallet.cons.Constants;
import com.beadwallet.cons.SQL;
import com.beadwallet.dao.DataDictionaryDao;
import com.beadwallet.dao.entity.DataDictionaryEntity;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import com.beadwallet.utils.common.CommonUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DataDictionaryDaoImpl extends BaseDao implements DataDictionaryDao {
    private static final Logger logger = LoggerFactory.getLogger(DataDictionaryDaoImpl.class);
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
    public List<DataDictionaryEntity> selectDictionaryInfo(Object[] param) {
        Connection connection = getConnection();
        if (connection == null) {
            return null;
        }

        PreparedStatement pstmt = null;
        ResultSet rs = null;

        List<DataDictionaryEntity> entityList = new ArrayList<>();
        try {
            String sql = SQL.DATA_DICTIONARY_SELECT;
            pstmt = connection.prepareStatement(sql);
            logger.info("sql=" + sql);
            if (param != null) {
                for (int i = 0; i < param.length; i++) {
                    pstmt.setObject(i + 1, param[i]);
                    logger.info(String.format("param%d=%s", i, param[i]));
                }
            }

            rs = pstmt.executeQuery();

            while(rs.next()) {
                DataDictionaryEntity entity = new DataDictionaryEntity();
                entity.setId(rs.getInt("id"));
                entity.setBusiness_source(rs.getString("business_source"));
                entity.setDb_source(rs.getString("db_source"));
                entity.setDb_name(rs.getString("db_name"));
                entity.setTable_name(rs.getString("table_name"));
                entity.setData_length(rs.getLong("data_length"));
                entity.setTable_comment(rs.getString("table_comment"));
                entity.setTime_offset(rs.getInt("time_offset"));
                entity.setLast_update(rs.getDate("last_update"));
                entity.setCurrent_ddl(rs.getBoolean("current_ddl"));
                entity.setLoad2hive(rs.getBoolean("load2hive"));
                if (CommonUtil.isExistColumn(rs, "level")) {
                    entity.setLevel(rs.getInt("level"));
                }
                if (CommonUtil.isExistColumn(rs, "tier")) {
                    entity.setTier(rs.getString("tier"));
                }
                entity.setIncreate_num(rs.getLong("increase_num"));
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

    @Override
    public boolean updateToLogicDelete(Object[] param) {
        Connection connection = getConnection();
        if (connection == null) {
            return false;
        }

        PreparedStatement ps = null;
        try {
            String sql = SQL.DATA_DICTIONARY_UPDATE;
            ps = connection.prepareStatement(sql);
            logger.info("sql=" + sql);
            if (param != null) {
                for (int i = 0; i < param.length; i++) {
                    ps.setObject(i + 1, param[i]);
                    logger.info(String.format("param%d=%s", i, param[i]));
                }
            }
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