package com.beadwallet.dao.impl;

import com.beadwallet.cons.Constants;
import com.beadwallet.cons.SQL;
import com.beadwallet.dao.AzkabanProjectsDao;
import com.beadwallet.dao.entity.ActiveExecutingFlowsEntity;
import com.beadwallet.dao.entity.AzkabanProjectsEntity;

import com.beadwallet.dao.entity.RetryInfoEntity;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class AzkabanProjectsDaoImpl extends BaseDao implements AzkabanProjectsDao {
    private static Connection connection = null;

    @Override
    public Connection getConnection() {
        if (connection != null) {
            return connection;
        }
        return getConnection(Constants.JDBC_TAG_AZKABAN);
    }

    @Override
    public void close() {
        closeConnection(connection);
    }

    @Override
    public List<AzkabanProjectsEntity> selectAzkabanProjects() {
        Connection connection = getConnection();
        if (connection == null) {
            return null;
        }

        PreparedStatement pstmt = null;
        ResultSet rs = null;

        List<AzkabanProjectsEntity> entityList = new ArrayList<>();
        try {
            pstmt = connection.prepareStatement(SQL.AZKABAN_PROJECTS_SELECT);
            rs = pstmt.executeQuery();

            while(rs.next()) {
                AzkabanProjectsEntity entity = new AzkabanProjectsEntity();
                entity.setName(rs.getString("name"));
                entity.setDescription(rs.getString("description"));
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

    public List<RetryInfoEntity> selectRetryFlowInfo(String projectPrefix) {
        Connection connection = getConnection();
        if (connection == null) {
            return null;
        }

        PreparedStatement pstmt = null;
        ResultSet rs = null;

        List<RetryInfoEntity> entityList = new ArrayList<>();
        try {
            pstmt = connection.prepareStatement(makeRetrySql(projectPrefix));
            rs = pstmt.executeQuery();

            while(rs.next()) {
                RetryInfoEntity entity = new RetryInfoEntity();
                entity.setProjectName(rs.getString("name"));
                entity.setFlowName(rs.getString("flow_id"));
                entity.setFlowStatus(rs.getString("status"));
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

    public List<ActiveExecutingFlowsEntity> selectPreparingExecId() {
        Connection connection = getConnection();
        if (connection == null) {
            return null;
        }

        PreparedStatement pstmt = null;
        ResultSet rs = null;

        List<ActiveExecutingFlowsEntity> entityList = new ArrayList<>();
        try {
            pstmt = connection.prepareStatement(SQL.SELECT_PREPARING_EXEC_ID);
            rs = pstmt.executeQuery();

            while(rs.next()) {
                ActiveExecutingFlowsEntity entity = new ActiveExecutingFlowsEntity();
                entity.setExecId(rs.getString("exec_id"));
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

    private String makeRetrySql(String prefix) {
        return SQL.AZKABAN_RETRY_FLOW_INFO + " AND t3.`name` LIKE ('" + prefix + "%')";
    }
}