package com.beadwallet.dao;

import com.beadwallet.dao.entity.CronDictionaryEntity;
import com.beadwallet.dao.entity.MetaExecuteEntity;
import java.sql.Connection;
import java.util.List;

public interface MetaExecuteDao {
    public Connection getConnection();
    public void close();
    public boolean insert(MetaExecuteEntity entity);
}
