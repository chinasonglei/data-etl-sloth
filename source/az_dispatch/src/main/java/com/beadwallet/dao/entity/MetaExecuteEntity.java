package com.beadwallet.dao.entity;

import java.sql.Date;

public class MetaExecuteEntity {
    private String module;
    private boolean status;
    private Date date;

    public String getModule() {
        return module;
    }

    public void setModule(String module) {
        this.module = module;
    }

    public boolean isStatus() {
        return status;
    }

    public void setStatus(boolean status) {
        this.status = status;
    }

    public Date getDate() {
        return date;
    }

    public void setDate(Date date) {
        this.date = date;
    }

    public String toString() {
        return String.format("module=%s, status=%s, data=%s", module, status, date);
    }
}
