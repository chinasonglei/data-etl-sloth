package com.beadwallet.dao.entity;

import java.sql.Date;

public class DataDictionaryEntity {
    private int id;
    private String business_source;
    private String db_source;
    private String db_name;
    private String table_name;
    private long data_length;
    private String table_comment;
    private int time_offset;
    private Date last_update;
    private boolean current_ddl;
    private boolean load2hive;
    private int level = 0;
    private String tier = "";
    private long increate_num;

    public DataDictionaryEntity() {

    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getBusiness_source() {
        return business_source;
    }

    public void setBusiness_source(String business_source) {
        this.business_source = business_source;
    }

    public String getDb_source() {
        return db_source;
    }

    public void setDb_source(String db_source) {
        this.db_source = db_source;
    }

    public String getDb_name() {
        return db_name;
    }

    public void setDb_name(String db_name) {
        this.db_name = db_name;
    }

    public String getTable_name() {
        return table_name;
    }

    public void setTable_name(String table_name) {
        this.table_name = table_name;
    }

    public long getData_length() {
        return data_length;
    }

    public void setData_length(long data_length) {
        this.data_length = data_length;
    }

    public String getTable_comment() {
        return table_comment;
    }

    public void setTable_comment(String table_comment) {
        this.table_comment = table_comment;
    }

    public int getTime_offset() {
        return time_offset;
    }

    public void setTime_offset(int time_offset) {
        this.time_offset = time_offset;
    }

    public Date getLast_update() {
        return last_update;
    }

    public void setLast_update(Date last_update) {
        this.last_update = last_update;
    }

    public boolean isCurrent_ddl() {
        return current_ddl;
    }

    public void setCurrent_ddl(boolean current_ddl) {
        this.current_ddl = current_ddl;
    }

    public boolean isLoad2hive() {
        return load2hive;
    }

    public void setLoad2hive(boolean load2hive) {
        this.load2hive = load2hive;
    }

    public long getIncreate_num() {
        return increate_num;
    }

    public void setIncreate_num(long increate_num) {
        this.increate_num = increate_num;
    }

    public int getLevel() {
        return level;
    }

    public void setLevel(int level) {
        this.level = level;
    }

    public String getTier() {
        return tier;
    }

    public void setTier(String tier) {
        this.tier = tier;
    }

    public String toString() {
        return "id=" + id
            + ", business_source=" + business_source
            + ", db_source=" + db_source
            + ", db_name=" + db_name
            + ", table_name=" + table_name
            + ", data_length=" + data_length
            + ", table_comment=" + table_comment
            + ", time_offset=" + time_offset
            + ", last_update=" + last_update
            + ", current_ddl=" + current_ddl
            + ", load2hive=" + load2hive
            + ", level=" + level
            + ", tier=" + tier
            + ", increate_num=" + increate_num
            + "\n";
    }
}
