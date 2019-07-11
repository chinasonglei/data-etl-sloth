package com.beadwallet.common.entity;

import java.io.Serializable;
import java.sql.Clob;
import java.sql.Date;

/**
 * @ClassName DataSchemaDetailEntity
 * @Description
 * @Author kai wu
 * @Date 2019/1/16 13:13
 * @Version 1.0
 */
public class DataSchemaDetailEntity implements Serializable{
    private int id;
    private String businessSource;
    private String dbName;
    private String tableName;
    private int ordinalPosition;
    private String columnName;
    private String columnType;
    private String columnComment;
    private int columnKey;
    private int isCreateTime;
    private int isUpdateTime;
    private int sensitiveData;
    private Date updateTime;

    public DataSchemaDetailEntity() {
    }

    public String getBusinessSource() {
        return businessSource;
    }

    public void setBusinessSource(String businessSource) {
        this.businessSource = businessSource;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
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

    public int getOrdinalPosition() {
        return ordinalPosition;
    }

    public void setOrdinalPosition(int ordinalPosition) {
        this.ordinalPosition = ordinalPosition;
    }

    public String getColumnName() {
        return columnName;
    }

    public void setColumnName(String columnName) {
        this.columnName = columnName;
    }

    public String getColumnType() {
        return columnType;
    }

    public void setColumnType(String columnType) {
        this.columnType = columnType;
    }

    public String getColumnComment() {
        return columnComment;
    }

    public void setColumnComment(String columnComment) {
        this.columnComment = columnComment;
    }

    public int getColumnKey() {
        return columnKey;
    }

    public void setColumnKey(int columnKey) {
        this.columnKey = columnKey;
    }

    public int getIsCreateTime() {
        return isCreateTime;
    }

    public void setIsCreateTime(int isCreateTime) {
        this.isCreateTime = isCreateTime;
    }

    public int getIsUpdateTime() {
        return isUpdateTime;
    }

    public void setIsUpdateTime(int isUpdateTime) {
        this.isUpdateTime = isUpdateTime;
    }

    public int getSensitiveData() {
        return sensitiveData;
    }

    public void setSensitiveData(int sensitiveData) {
        this.sensitiveData = sensitiveData;
    }

    public Date getUpdateTime() {
        return updateTime;
    }

    public void setUpdateTime(Date updateTime) {
        this.updateTime = updateTime;
    }
}
