package com.beadwallet.dao;

import com.beadwallet.dao.entity.CronDictionaryEntity;

import java.sql.Connection;
import java.util.List;

public interface CronDictionaryDao {
    public Connection getConnection();
    public void close();
    public List<CronDictionaryEntity> selectCronDicInfo();
}
