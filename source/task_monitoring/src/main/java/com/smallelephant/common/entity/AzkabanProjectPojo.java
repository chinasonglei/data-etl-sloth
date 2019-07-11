package com.smallelephant.common.entity;


import java.io.Serializable;

public class AzkabanProjectPojo implements Serializable {
    private int id;
    private int projectID;
    private String flowId;
    private Long startTime;
    private Long endTime;
    private int status;

    public int getProjectID() {
        return projectID;
    }

    public void setProjectID(int projectID) {
        this.projectID = projectID;
    }

    public Long getStartTime() {
        return startTime;
    }

    public void setStartTime(Long startTime) {
        this.startTime = startTime;
    }

    public Long getEndTime() {
        return endTime;
    }

    public void setEndTime(Long endTime) {
        this.endTime = endTime;
    }

    public AzkabanProjectPojo(int id, int projectID, String flowId, Long startTime, Long endTime, int status) {
        this.id = id;
        this.projectID = projectID;
        this.flowId = flowId;
        this.startTime = startTime;
        this.endTime = endTime;
        this.status = status;
    }

    @Override
    public String toString() {
        return "AzkabanProjectPojo{" +
                "id=" + id +
                ", projectID=" + projectID +
                ", flowId='" + flowId + '\'' +
                ", startTime=" + startTime +
                ", endTime=" + endTime +
                ", status=" + status +
                '}';
    }

    public AzkabanProjectPojo() {
    }

    public String getFlowId() {
        return flowId;
    }

    public void setFlowId(String flowId) {
        this.flowId = flowId;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public int getStatus() {
        return status;
    }

    public void setStatus(int status) {
        this.status = status;
    }
}
