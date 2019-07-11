package com.beadwallet.dao.entity;

import java.sql.Date;

public class DispatchFlowInfoEntity {
    private String project_type;
    private String flow_name;
    private String project_name;
    private String business_source;
    private String db_source;
    private String db_name;
    private String table;
    private long data_length;
    private long increase_num;
    private int level;
    private int time_offset;
    private boolean current_ddl;
    private boolean load2hive;
    private Date last_update;
    private boolean delete;

    public DispatchFlowInfoEntity(){
    }

    public String getProject_name() {
        return project_name;
    }

    public void setProject_name(String project_name) {
        this.project_name = project_name;
    }

    public String getProject_type() {
        return project_type;
    }

    public void setProject_type(String project_type) {
        this.project_type = project_type;
    }

    public String getFlow_name() {
        return flow_name;
    }

    public void setFlow_name(String flow_name) {
        this.flow_name = flow_name;
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

    public String getTable() {
        return table;
    }

    public void setTable(String table) {
        this.table = table;
    }

    public long getData_length() {
        return data_length;
    }

    public void setData_length(long data_length) {
        this.data_length = data_length;
    }

    public long getIncrease_num() {
        return increase_num;
    }

    public void setIncrease_num(long increase_num) {
        this.increase_num = increase_num;
    }

    public int getLevel() {
        return level;
    }

    public void setLevel(int level) {
        this.level = level;
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

    public boolean isDelete() {
        return delete;
    }

    public void setDelete(boolean delete) {
        this.delete = delete;
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

    public String toString() {
        return " project_type=" + project_type
            + " ,flow_name=" + flow_name
            + " ,project_name=" + project_name
            + " ,business_source=" + business_source
            + " ,db_source=" + db_source
            + " ,db_name=" + db_name
            + " ,table=" + table
            + " ,data_length=" + data_length
            + " ,increase_num=" + increase_num
            + " ,level=" + level
            + " ,time_offset=" + time_offset
            + " ,current_ddl=" + current_ddl
            + " ,load2hive=" + load2hive
            + " ,last_update=" + last_update
            + " \n";
    }
}
