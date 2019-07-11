package com.beadwallet.common.entity;

import java.io.Serializable;

/**
 * @ClassName TimeOffsetEntity
 * @Description
 * @Author kai wu
 * @Date 2019/1/22 15:57
 * @Version 1.0
 */
public class TimeOffsetEntity implements Serializable {
    private String dbSource;
    private String dbName;
    private String tableName;
    private String timeOffset;

    public TimeOffsetEntity() {
    }

    public String getDbSource() {
        return dbSource;
    }

    public void setDbSource(String dbSource) {
        this.dbSource = dbSource;
    }

    public String getDbName() {
        return dbName;
    }

    public void setDbName(String dbName) {
        this.dbName = dbName;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getTimeOffset() {
        return timeOffset;
    }

    public void setTimeOffset(String timeOffset) {
        this.timeOffset = timeOffset;
    }
}
