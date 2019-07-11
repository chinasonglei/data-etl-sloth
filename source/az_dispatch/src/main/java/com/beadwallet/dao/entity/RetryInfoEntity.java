package com.beadwallet.dao.entity;

public class RetryInfoEntity {
    private String projectName;
    private String flowName;
    private String flowStatus;

    public String getProjectName() {
        return projectName;
    }

    public void setProjectName(String projectName) {
        this.projectName = projectName;
    }

    public String getFlowName() {
        return flowName;
    }

    public void setFlowName(String flowName) {
        this.flowName = flowName;
    }

    public String getFlowStatus() {
        return flowStatus;
    }

    public void setFlowStatus(String flowStatus) {
        this.flowStatus = flowStatus;
    }

    public String toString() {
        return "projectName=" + projectName
                + ", flowName=" + flowName
                + ", flowStatus=" + flowStatus
                + "\n";
    }
}
