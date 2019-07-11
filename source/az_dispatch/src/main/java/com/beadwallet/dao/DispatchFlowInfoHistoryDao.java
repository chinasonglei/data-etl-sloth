package com.beadwallet.dao;

import java.sql.Connection;

public interface DispatchFlowInfoHistoryDao {
    public Connection getConnection();
    public void close();
    public boolean insertHistory();
}