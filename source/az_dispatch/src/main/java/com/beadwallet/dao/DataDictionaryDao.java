package com.beadwallet.dao;

import com.beadwallet.dao.entity.DataDictionaryEntity;

import java.sql.Connection;
import java.util.List;

public interface DataDictionaryDao {
    public Connection getConnection();
    public void close();
    public List<DataDictionaryEntity> selectDictionaryInfo(Object[] param);
    public boolean updateToLogicDelete(Object[] param);
}