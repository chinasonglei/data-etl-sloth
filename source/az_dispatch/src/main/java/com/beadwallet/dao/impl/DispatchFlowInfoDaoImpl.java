package com.beadwallet.dao.impl;

import com.beadwallet.cons.Constants;
import com.beadwallet.cons.SQL;
import com.beadwallet.dao.DispatchFlowInfoDao;
import com.beadwallet.dao.entity.DispatchFlowInfoEntity;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DispatchFlowInfoDaoImpl extends BaseDao implements DispatchFlowInfoDao {
    private static final Logger logger = LoggerFactory.getLogger(DispatchFlowInfoDaoImpl.class);
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
    public List<DispatchFlowInfoEntity> selectDispatchFlowInfo(Object[] param, String dispatchTier) {
        Connection connection = getConnection();
        if (connection == null) {
            return null;
        }

        PreparedStatement pstmt = null;
        ResultSet rs = null;

        List<DispatchFlowInfoEntity> entityList = new ArrayList<>();
        try {
            String sql = SQL.DISPATCH_FLOW_INFO_SELECT + makeWhereTier(dispatchTier);
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
                DispatchFlowInfoEntity entity = new DispatchFlowInfoEntity();
                entity.setProject_type(rs.getString("project_type"));
                entity.setFlow_name(rs.getString("flow_name"));
                entity.setProject_name(rs.getString("project_name"));
                entity.setBusiness_source(rs.getString("business_source"));
                entity.setDb_source(rs.getString("db_source"));
                entity.setDb_name(rs.getString("db_name"));
                entity.setTable(rs.getString("table"));
                entity.setIncrease_num(rs.getLong("increase_num"));
                entity.setLevel(rs.getInt("level"));
                entity.setTime_offset(rs.getInt("time_offset"));
                entity.setCurrent_ddl(rs.getBoolean("current_ddl"));
                entity.setLoad2hive(rs.getBoolean("load2hive"));
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
    public boolean batchInsert(List<DispatchFlowInfoEntity> entityList) {
        if (entityList == null || entityList.size() <= 0) {
            return false;
        }

        Connection connection = getConnection();
        if (connection == null) {
            return false;
        }

        PreparedStatement ps = null;
        try {
            connection.setAutoCommit(false);

            for (DispatchFlowInfoEntity entity : entityList) {
                ps = connection.prepareStatement(SQL.DISPATCH_FLOW_INFO_INSERT);
                int i = 1;
                //VALUES
                ps.setString(i++, entity.getProject_type());
                ps.setString(i++, entity.getFlow_name());
                ps.setString(i++, entity.getProject_name());
                ps.setString(i++, entity.getBusiness_source());
                ps.setString(i++, entity.getDb_source());
                ps.setString(i++, entity.getDb_name());
                ps.setString(i++, entity.getTable());
                ps.setLong(i++, entity.getIncrease_num());
                ps.setInt(i++, entity.getLevel());
                ps.setInt(i++, entity.getTime_offset());
                ps.setBoolean(i++, entity.isCurrent_ddl());
                ps.setBoolean(i++, entity.isLoad2hive());
                ps.setDate(i++, entity.getLast_update());
                //ON DUPLICATE KEY UPDATE
                ps.setString(i++, entity.getProject_name());
                ps.setString(i++, entity.getBusiness_source());
                ps.setString(i++, entity.getDb_source());
                ps.setString(i++, entity.getDb_name());
                ps.setString(i++, entity.getTable());
                ps.setLong(i++, entity.getIncrease_num());
                ps.setInt(i++, entity.getTime_offset());
                ps.setBoolean(i++, entity.isCurrent_ddl());
                ps.setBoolean(i++, entity.isLoad2hive());
                ps.setDate(i, entity.getLast_update());

                ps.execute();
            }
            connection.commit();

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

    @Override
    public boolean batchDelete(List<DispatchFlowInfoEntity> entityList) {
        if (entityList == null || entityList.size() <= 0) {
            return false;
        }

        Connection connection = getConnection();
        if (connection == null) {
            return false;
        }

        PreparedStatement ps = null;
        try {
            connection.setAutoCommit(false);
            String sql = SQL.DISPATCH_FLOW_INFO_DELETE;
            ps = connection.prepareStatement(sql);
            logger.info("sql=" + sql);
            for (DispatchFlowInfoEntity entity : entityList) {
                ps.setString(1, entity.getProject_type());
                logger.debug("project_type=" + entity.getProject_type());
                ps.setString(2, entity.getFlow_name());
                logger.debug("flow_name=" + entity.getFlow_name());
                ps.execute();
            }
            connection.commit();
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

    /**
     * 拼接调度层级的附加检索条件
     *
     * @param dispatchTier 调度层级
     * @return 附加检索条件
     */
    private String makeWhereTier(String dispatchTier) {
        StringBuilder result = new StringBuilder("");

        String[] tiers = dispatchTier.split(",");
        result.append(" AND (");
        int index = 0;
        for (String tier : tiers) {
            if (index > 0) {
                result.append(" OR ");
            }
            index++;
            result.append(" flow_name like 'etl_").append(tier).append("%' ");
        }
        result.append(" ) ");
        return result.toString();
    }
}