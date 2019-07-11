package com.beadwallet.dao;

import com.beadwallet.dao.entity.ActiveExecutingFlowsEntity;
import com.beadwallet.dao.entity.AzkabanProjectsEntity;

import com.beadwallet.dao.entity.RetryInfoEntity;
import java.sql.Connection;
import java.util.List;

public interface AzkabanProjectsDao {
    public Connection getConnection();
    public void close();
    public List<AzkabanProjectsEntity> selectAzkabanProjects();
    public List<RetryInfoEntity> selectRetryFlowInfo(String projectPrefix);
    public List<ActiveExecutingFlowsEntity> selectPreparingExecId();
}