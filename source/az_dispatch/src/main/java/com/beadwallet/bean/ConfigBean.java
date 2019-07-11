package com.beadwallet.bean;

public class ConfigBean {
    private String azkabanUrl;
    private String azkabanUsername;
    private String azkabanPassword;
    private String autoGenerationPath;
    private String etlProjectPrefix;
    private String etlPrepareProjectPrefix;
    private String etlPrepareFolder;
    private String etlPrepareBatchNumber;
    private String etlPrepareInterval;
    private String etlScriptFolder;
    private String etlBatchNumber;
    private String etlFirstHours;
    private String etlFirstMinute;
    private String dispatchTier;
    private String retryBatchNumber;
    private String retryInterval;

    public ConfigBean() {
    }

    public ConfigBean(String azkaban_url,
                      String azkaban_username,
                      String azkaban_password,
                      String auto_generation_path,
                      String etl_project_prefix,
                      String etl_prepare_project_prefix,
                      String etl_prepare_folder,
                      String etl_prepare_batch_num,
                      String etl_prepare_interval,
                      String etl_batch_number,
                      String etl_first_hours,
                      String etl_first_minute,
                      String dispatch_tier,
                      String retryBatchNumber,
                      String retryInterval) {
        this.azkabanUrl = azkaban_url;
        this.azkabanUsername = azkaban_username;
        this.azkabanPassword = azkaban_password;
        this.autoGenerationPath = auto_generation_path;
        this.etlProjectPrefix = etl_project_prefix;
        this.etlPrepareProjectPrefix = etl_prepare_project_prefix;
        this.etlPrepareFolder = etl_prepare_folder;
        this.etlPrepareBatchNumber = etl_prepare_batch_num;
        this.etlPrepareInterval = etl_prepare_interval;
        this.etlBatchNumber = etl_batch_number;
        this.etlFirstHours = etl_first_hours;
        this.etlFirstMinute = etl_first_minute;
        this.dispatchTier = dispatch_tier;
        this.retryBatchNumber = retryBatchNumber;
        this.retryInterval = retryInterval;
    }

    public String getAzkabanUrl() {
        return azkabanUrl;
    }

    public void setAzkabanUrl(String azkabanUrl) {
        this.azkabanUrl = azkabanUrl;
    }

    public String getAzkabanUsername() {
        return azkabanUsername;
    }

    public void setAzkabanUsername(String azkabanUsername) {
        this.azkabanUsername = azkabanUsername;
    }

    public String getAzkabanPassword() {
        return azkabanPassword;
    }

    public void setAzkabanPassword(String azkabanPassword) {
        this.azkabanPassword = azkabanPassword;
    }

    public String getAutoGenerationPath() {
        return autoGenerationPath;
    }

    public void setAutoGenerationPath(String autoGenerationPath) {
        this.autoGenerationPath = autoGenerationPath;
    }

    public String getEtlProjectPrefix() {
        return etlProjectPrefix;
    }

    public void setEtlProjectPrefix(String etlProjectPrefix) {
        this.etlProjectPrefix = etlProjectPrefix;
    }

    public String getEtlPrepareBatchNumber() {
        return etlPrepareBatchNumber;
    }

    public void setEtlPrepareBatchNumber(String etlPrepareBatchNumber) {
        this.etlPrepareBatchNumber = etlPrepareBatchNumber;
    }

    public String getEtlPrepareInterval() {
        return etlPrepareInterval;
    }

    public void setEtlPrepareInterval(String etlPrepareInterval) {
        this.etlPrepareInterval = etlPrepareInterval;
    }

    public String getEtlPrepareFolder() {
        return etlPrepareFolder;
    }

    public void setEtlPrepareFolder(String etlPrepareFolder) {
        this.etlPrepareFolder = etlPrepareFolder;
    }

    public String getEtlScriptFolder() {
        return etlScriptFolder;
    }

    public void setEtlScriptFolder(String etlScriptFolder) {
        this.etlScriptFolder = etlScriptFolder;
    }

    public String getEtlBatchNumber() {
        return etlBatchNumber;
    }

    public void setEtlBatchNumber(String etlBatchNumber) {
        this.etlBatchNumber = etlBatchNumber;
    }

    public String getEtlFirstHours() {
        return etlFirstHours;
    }

    public void setEtlFirstHours(String etlFirstHours) {
        this.etlFirstHours = etlFirstHours;
    }

    public String getEtlFirstMinute() {
        return etlFirstMinute;
    }

    public void setEtlFirstMinute(String etlFirstMinute) {
        this.etlFirstMinute = etlFirstMinute;
    }

    public String getEtlPrepareProjectPrefix() {
        return etlPrepareProjectPrefix;
    }

    public void setEtlPrepareProjectPrefix(String etlPrepareProjectPrefix) {
        this.etlPrepareProjectPrefix = etlPrepareProjectPrefix;
    }

    public String getDispatchTier() {
        return dispatchTier;
    }

    public void setDispatchTier(String dispatchTier) {
        this.dispatchTier = dispatchTier;
    }

    public String getRetryBatchNumber() {
        return retryBatchNumber;
    }

    public void setRetryBatchNumber(String retryBatchNumber) {
        this.retryBatchNumber = retryBatchNumber;
    }

    public String getRetryInterval() {
        return retryInterval;
    }

    public void setRetryInterval(String retryInterval) {
        this.retryInterval = retryInterval;
    }
}
