package com.smallelephant.common.entity;

import java.io.Serializable;

public class RetMongoPojo implements Serializable {

    private String id;
    private String tname;
    private int count;
    private String start_time;
    private String end_time;
    private String time_diff;
    private String type;
    private int status;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getTname() {
        return tname;
    }

    public void setTname(String tname) {
        this.tname = tname;
    }

    public int getCount() {
        return count;
    }

    public void setCount(int count) {
        this.count = count;
    }

    public String getStart_time() {
        return start_time;
    }

    public void setStart_time(String start_time) {
        this.start_time = start_time;
    }

    public String getEnd_time() {
        return end_time;
    }

    public void setEnd_time(String end_time) {
        this.end_time = end_time;
    }

    public String getTime_diff() {
        return time_diff;
    }

    public void setTime_diff(String time_diff) {
        this.time_diff = time_diff;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public int getStatus() {
        return status;
    }

    public void setStatus(int status) {
        this.status = status;
    }

    @Override
    public String toString() {
        return "RetMongoPojo{" +
                "id='" + id + '\'' +
                ", tname='" + tname + '\'' +
                ", count=" + count +
                ", start_time='" + start_time + '\'' +
                ", end_time='" + end_time + '\'' +
                ", time_diff='" + time_diff + '\'' +
                ", type='" + type + '\'' +
                ", status=" + status +
                '}';
    }

    public static String sendEmailTitle() {
        return "<table border=\"1\"><tr>" + "<th>ID</th>" + "<th>表名</th>" +"<th>数量</th>"+ "<th>开始时间</th>" + "<th>结束时间</th>" + "<th>耗时/s</th>" + "<th>类型</th>" + "<th>状态</th>" + "</tr>";
    }

    public String sendEmailMessage() {
        return "<tr><td>" + id + "</td><td>" + tname + "</td><td>" + count + "</td><td>" + start_time + "</td><td>" + end_time + "</td><td>" + time_diff + "</td><td>" + type +
                "</td><td>" + status + "</td><tr>";
    }
}
