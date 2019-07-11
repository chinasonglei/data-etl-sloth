package com.beadwallet.dao.entity;

public class CronDictionaryEntity {
    private int id;
    private int min;
    private int hours;
    private String day_of_month;
    private String month;
    private String day_of_week;
    private boolean delete;
    private String cron_str;

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public int getMin() {
        return min;
    }

    public void setMin(int min) {
        this.min = min;
    }

    public int getHours() {
        return hours;
    }

    public void setHours(int hours) {
        this.hours = hours;
    }

    public String getDay_of_month() {
        return day_of_month;
    }

    public void setDay_of_month(String day_of_month) {
        this.day_of_month = day_of_month;
    }

    public String getMonth() {
        return month;
    }

    public void setMonth(String month) {
        this.month = month;
    }

    public String getDay_of_week() {
        return day_of_week;
    }

    public void setDay_of_week(String day_of_week) {
        this.day_of_week = day_of_week;
    }

    public boolean isDelete() {
        return delete;
    }

    public void setDelete(boolean delete) {
        this.delete = delete;
    }

    public String getCron_str() {
        return cron_str;
    }

    public void setCron_str(String cron_str) {
        this.cron_str = cron_str;
    }

    public String toString() {
        return "id=" + id
            + " ,min=" + min
            + " ,hours=" + hours
            + " ,day_of_month=" + day_of_month
            + " ,month=" + month
            + " ,day_of_week=" + day_of_week
            + " ,cron_str=" + cron_str;
    }
}
