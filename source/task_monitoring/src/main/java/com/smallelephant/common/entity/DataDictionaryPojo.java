package com.smallelephant.common.entity;

import java.io.Serializable;
import java.sql.Date;

public class DataDictionaryPojo implements Serializable {

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
    private int storage;

    /**
     * ods、rds的保留字段
     */
    private int ods_success;
    private int rds_success;

    public int getOds_success() {
        return ods_success;
    }

    public void setOds_success(int ods_success) {
        this.ods_success = ods_success;
    }

    public int getRds_success() {
        return rds_success;
    }

    public void setRds_success(int rds_success) {
        this.rds_success = rds_success;
    }

    public DataDictionaryPojo() {
    }

    public int getStorage() {
        return storage;
    }

    public void setStorage(int storage) {
        this.storage = storage;
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

    @Override
    public String toString() {
        return "DataDictionaryPojo{" +
                "id=" + id +
                ", businessSource='" + businessSource + '\'' +
                ", dbSource='" + dbSource + '\'' +
                ", dbName='" + dbName + '\'' +
                ", tableName='" + tableName + '\'' +
                ", dataLength=" + dataLength +
                ", tableComment='" + tableComment + '\'' +
                ", timeOffset=" + timeOffset +
                ", lastUpdate=" + lastUpdate +
                ", currentDdl=" + currentDdl +
                ", load2hive=" + load2hive +
                ", delete=" + delete +
                ", storage=" + storage +
                '}';
    }

    public String sendEmailTitle() {
        return "<table border=\"1\"><tr>" + "<th>ID</th>" + "<th>业务源</th>" + "<th>dbSource</th>" + "<th>dbName</th>" + "<th>tableName</th>" + "<th>ods_success</th>" + "<th>rds_success</th>" +
                "<th>currentDdl</th>" + "<th>load2hive</th>" + "<th>storage</th>" + "<th>comment</th>" + "</tr>";
    }

    public String sendEmailMessage() {
        return "<tr><td>" + id + "</td><td>" + businessSource + "</td><td>" + dbSource + "</td><td>" + dbName + "</td><td>" + tableName + "</td><td>" + ods_success + "</td><td>" + rds_success +
                "</td><td>" + currentDdl + "</td><td>" + load2hive + "</td><td>" + storage + "</td><td>" + tableComment +
                "</td></tr>";
    }

}
