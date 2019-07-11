package com.beadwallet.dao;

import com.beadwallet.dao.entity.DispatchFlowInfoEntity;

import java.sql.Connection;
import java.util.List;

public interface DispatchFlowInfoDao {
    public Connection getConnection();
    public void close();
    public List<DispatchFlowInfoEntity> selectDispatchFlowInfo(Object[] param, String dispatchTier);
    public boolean batchInsert(List<DispatchFlowInfoEntity> entityList);
    public boolean batchDelete(List<DispatchFlowInfoEntity> entityList);
}