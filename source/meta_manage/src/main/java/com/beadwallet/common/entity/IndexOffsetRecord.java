package com.beadwallet.common.entity;

/**
 * @ClassName IndexOffsetRecord
 * @Description
 * @Author kai wu
 * @Date 2019/3/22 12:45
 * @Version 1.0
 */
public class IndexOffsetRecord {
    private int id;
    private String businessSource;
    private String dbName;
    private String tableName;
    private String columnKey;
    private long offSet;
    private String insertTime;

    public IndexOffsetRecord() {
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

    public String getColumnKey() {
        return columnKey;
    }

    public void setColumnKey(String columnKey) {
        this.columnKey = columnKey;
    }

    public long getOffSet() {
        return offSet;
    }

    public void setOffSet(long offSet) {
        this.offSet = offSet;
    }

    public String getInsertTime() {
        return insertTime;
    }

    public void setInsertTime(String insertTime) {
        this.insertTime = insertTime;
    }
}
