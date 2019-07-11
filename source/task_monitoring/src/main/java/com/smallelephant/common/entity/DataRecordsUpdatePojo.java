package com.smallelephant.common.entity;


import java.io.Serializable;
import java.sql.Date;

public class DataRecordsUpdatePojo implements Serializable {

    private int id;
    private String business_source;
    private String db_name;
    private String table_name;
    private long increase_num;
    private long export_num;
    private boolean update_status;
    private Date update_date;

    /**
     * private int id;
     * private int currentDdl;
     * private String dbName;
     * private String tableName;
     * private int timeOffset;
     * private String createTime;
     * private String updateTime;
     * private int increaseNum;
     * private int exportNum;
     * private int updateStatus;
     * private Date updateDate;
     */
    public DataRecordsUpdatePojo() {
    }


    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getTable_name() {
        return table_name;
    }

    public void setTable_name(String table_name) {
        this.table_name = table_name;
    }

    public long getIncrease_num() {
        return increase_num;
    }

    public void setIncrease_num(long increase_num) {
        this.increase_num = increase_num;
    }

    public long getExport_num() {
        return export_num;
    }

    public void setExport_num(long export_num) {
        this.export_num = export_num;
    }

    public boolean isUpdate_status() {
        return update_status;
    }

    public void setUpdate_status(boolean update_status) {
        this.update_status = update_status;
    }

    public DataRecordsUpdatePojo(int id, String business_source, String db_name, String table_name, long increase_num, long export_num, boolean update_status, Date update_date) {
        this.id = id;
        this.business_source = business_source;
        this.db_name = db_name;
        this.table_name = table_name;
        this.increase_num = increase_num;
        this.export_num = export_num;
        this.update_status = update_status;
        this.update_date = update_date;
    }

    public Date getUpdate_date() {
        return update_date;
    }

    public void setUpdate_date(Date update_date) {
        this.update_date = update_date;
    }

    public String getBusiness_source() {
        return business_source;
    }

    public void setBusiness_source(String business_source) {
        this.business_source = business_source;
    }

    public String getDb_name() {
        return db_name;
    }

    public void setDb_name(String db_name) {
        this.db_name = db_name;
    }

    @Override
    public String toString() {
        return "DataRecordsUpdatePojo{" +
                "id=" + id +
                ", business_source='" + business_source + '\'' +
                ", db_name='" + db_name + '\'' +
                ", table_name='" + table_name + '\'' +
                ", increase_num=" + increase_num +
                ", export_num=" + export_num +
                ", update_status=" + update_status +
                ", update_date=" + update_date +
                '}';
    }


    public String sendEmailTitle() {
        return "<table border=\"1\"><tr>" + "<th>ID</th>" + "<th>业务源</th>" + "<th>db_name</th>" + "<th>table_name</th>" + "<th>increase_num</th>" + "<th>export_num</th>" + "<th>update_status</th>" + "<th>update_date</th>" + "</tr>";
    }

    public String sendEmailMessage() {
        return "<tr><td>" + id + "</td><td>" + business_source + "</td><td>" + db_name + "</td><td>" + table_name + "</td><td>" + increase_num +
                "</td><td>" + export_num + "</td><td>" + update_status + "</td><td>" + update_date + "</tr>";
    }
}
