package com.beadwallet.utils.config;

import com.alibaba.fastjson.JSONObject;
import com.beadwallet.bean.ConfigBean;
import com.beadwallet.cons.Constants;
import com.beadwallet.utils.xml.XMLReader;

/**
 * 配置文件通用类
 *
 * @author QuChunhui 2019/01/28
 */
public class ConfigUtil {
    private static ConfigUtil configUtil = null;
    private static JSONObject jsonObject = null;

    //JDBC配置文件路径
    public static final String CONFIG_PATH = "configPath";
    //Azkaban访问地址
    public static final String AZKABAN_URL = "azkabanUrl";
    //Azkaban访问用户名
    public static final String AZKABAN_USERNAME = "azkabanUsername";
    //Azkaban访问密码
    public static final String AZKABAN_PASSWORD = "azkabanPassword";
    //自动生成脚本文件所在路径
    public static final String AUTO_GENERATION_PATH = "autoGenerationPath";
    //ETL工程的前缀名称
    public static final String ETL_PROJECT_PREFIX = "etlProjectPrefix";
    //ETL元数据更新工程的前缀名称
    public static final String ETL_PREPARE_PROJECT_PREFIX = "etlPrepareProjectPrefix";
    //ETL元数据更新工程每批次执行数量
    public static final String ETL_PREPARE_BATCH_NUMBER = "etlPrepareBatchNumber";
    //ETL元数据更新工程单位: 毫秒
    public static final String ETL_PREPARE_INTERNAL = "etlPrepareInterval";
    //ETL元数据更新工程文件所在路径
    public static final String ETL_PREPARE_FOLDER = "etlPrepareFolder";
    //ETL工程文件所在路径
    public static final String ETL_SCRIPT_FOLDER = "etlScriptFolder";
    //ETL每批调度的任务数量
    public static final String ETL_BATCH_NUMBER = "etlBatchNumber";
    //ETL调度任务开始时间
    public static final String ETL_FIRST_HOURS = "etlFirstHours";
    //ETL调度任务开始分钟
    public static final String ETL_FIRST_MINUTE = "etlFirstMinute";
    //ETL调度任务层级范围
    public static final String DISPATCH_TIER = "dispatchTier";
    //失败重试每批次执行数量
    public static final String RETRY_BATCH_NUMBER = "retryBatchNumber";
    //失败重试时间间隔单位: 毫秒
    public static final String RETRY_INTERNAL = "retryInterval";

    private ConfigUtil() {
        //nothing
    }

    public static ConfigUtil getInstance() {
        if (configUtil == null) {
            configUtil = new ConfigUtil();
        }
        return configUtil;
    }

    public void loadProperties(String configPath) {
        ConfigBean configBean = (ConfigBean)XMLReader.getXMInfo(
            Constants.AZ_DISPATCH_BEAN_CLASS_NAME,
            configPath,
            Constants.AZ_DISPATCH_TAG_NAME,
            1);

        if (configBean == null) {
            return;
        }

        jsonObject = new JSONObject();
        jsonObject.put(CONFIG_PATH, configPath);
        jsonObject.put(AZKABAN_URL, configBean.getAzkabanUrl());
        jsonObject.put(AZKABAN_USERNAME, configBean.getAzkabanUsername());
        jsonObject.put(AZKABAN_PASSWORD, configBean.getAzkabanPassword());
        jsonObject.put(AUTO_GENERATION_PATH, configBean.getAutoGenerationPath());
        jsonObject.put(ETL_PROJECT_PREFIX, configBean.getEtlProjectPrefix());
        jsonObject.put(ETL_PREPARE_PROJECT_PREFIX, configBean.getEtlPrepareProjectPrefix());
        jsonObject.put(ETL_PREPARE_BATCH_NUMBER, configBean.getEtlPrepareBatchNumber());
        jsonObject.put(ETL_PREPARE_INTERNAL, configBean.getEtlPrepareInterval());
        jsonObject.put(ETL_PREPARE_FOLDER, configBean.getEtlPrepareFolder());
        jsonObject.put(ETL_SCRIPT_FOLDER, configBean.getEtlScriptFolder());
        jsonObject.put(ETL_BATCH_NUMBER, configBean.getEtlBatchNumber());
        jsonObject.put(ETL_FIRST_HOURS, configBean.getEtlFirstHours());
        jsonObject.put(ETL_FIRST_MINUTE, configBean.getEtlFirstMinute());
        jsonObject.put(DISPATCH_TIER, configBean.getDispatchTier());
        jsonObject.put(RETRY_BATCH_NUMBER, configBean.getRetryBatchNumber());
        jsonObject.put(RETRY_INTERNAL, configBean.getRetryInterval());
    }

    public static String getProperties(String p) {
        if (jsonObject != null) {
            return jsonObject.getString(p);
        } else {
            return null;
        }
    }
}
