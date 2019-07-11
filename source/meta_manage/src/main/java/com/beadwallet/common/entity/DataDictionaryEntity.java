package com.beadwallet.common.entity;

import java.io.Serializable;
import java.sql.Date;

/**
 * @ClassName DataDictionaryEntity
 * @Description
 * @Author kai wu
 * @Date 2019/1/16 13:12
 * @Version 1.0
 */
public class DataDictionaryEntity implements Serializable{

    private int id;
    private String businessSource;
    private String dbSource;
    private String dbName;
    private String tableName;
    private long dataLength;
    private String tableComment;
    private int timeOffset;
    private Date lastUpdate;
    private int currentDdl;
    private int load2hive;
    private int delete;

    public DataDictionaryEntity() {
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getBusinessSource() {
        return businessSource;
    }

    public void setBusinessSource(String businessSource) {
        this.businessSource = businessSource;
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

    public long getDataLength() {
        return dataLength;
    }

    public void setDataLength(long dataLength) {
        this.dataLength = dataLength;
    }

    public String getTableComment() {
        return tableComment;
    }

    public void setTableComment(String tableComment) {
        this.tableComment = tableComment;
    }

    public int getTimeOffset() {
        return timeOffset;
    }

    public void setTimeOffset(int timeOffset) {
        this.timeOffset = timeOffset;
    }

    public Date getLastUpdate() {
        return lastUpdate;
    }

    public void setLastUpdate(Date lastUpdate) {
        this.lastUpdate = lastUpdate;
    }

    public int getCurrentDdl() {
        return currentDdl;
    }

    public void setCurrentDdl(int currentDdl) {
        this.currentDdl = currentDdl;
    }

    public int getLoad2hive() {
        return load2hive;
    }

    public void setLoad2hive(int load2hive) {
        this.load2hive = load2hive;
    }

    public int getDelete() {
        return delete;
    }

    public void setDelete(int delete) {
        this.delete = delete;
    }
}
