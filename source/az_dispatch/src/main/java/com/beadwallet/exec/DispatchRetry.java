package com.beadwallet.exec;

import com.beadwallet.dao.AzkabanProjectsDao;
import com.beadwallet.dao.entity.RetryInfoEntity;
import com.beadwallet.dao.impl.AzkabanProjectsDaoImpl;
import com.beadwallet.utils.azkaban.AzkabanApiUtil;
import com.beadwallet.utils.common.CommonUtil;
import com.beadwallet.utils.config.ConfigUtil;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DispatchRetry {
    private static final Logger logger = LoggerFactory.getLogger(DispatchRetry.class);
    private AzkabanProjectsDao azkabanProjectsDao = null;
    private AzkabanApiUtil azkabanApiUtil = null;
    private int retryBatchNumber = 1;
    private int retryInterval = 1;
    private String projectPrefixes;

    /**
     * 构造函数
     * @param configPath 配置文件路径
     * @param prefixes Azkaban Project前缀列表（逗号分隔）
     */
    private DispatchRetry(String configPath, String prefixes) {
        ConfigUtil configUtil = ConfigUtil.getInstance();
        configUtil.loadProperties(configPath);
        projectPrefixes = prefixes;
    }

    /**
     * 程序入口函数
     * @param args 参数1：配置文件路径。
     *              参数2：前缀列表（逗号分隔）
     */
    public static void main(String[] args) {
        if (args.length < 2) {
            return;
        }

        //对象初始化
        DispatchRetry retry = new DispatchRetry(args[0], args[1]);

        //初始化全局变量
        if (!retry.initialize()) {
            retry.close();
            return;
        }

        //程序主处理
        if (!retry.execute()) {
            retry.close();
            return;
        }

        //释放全局资源
        retry.close();
    }

    /**
     * 初始化全局变量
     * @return true：成功、false：异常
     */
    private boolean initialize() {
        //失败重试每批次执行数量
        String batchNumber = ConfigUtil.getProperties(ConfigUtil.RETRY_BATCH_NUMBER);
        if (batchNumber == null) {
            logger.error(String.format(
                "property %s is not exist.", ConfigUtil.RETRY_BATCH_NUMBER));
            return false;
        }
        retryBatchNumber = Integer.valueOf(batchNumber);

        //失败重试时间间隔单位: 毫秒
        String interval = ConfigUtil.getProperties(ConfigUtil.RETRY_INTERNAL);
        if (interval == null) {
            logger.error(String.format(
                "property %s is not exist.", ConfigUtil.RETRY_INTERNAL));
            return false;
        }
        retryInterval = Integer.valueOf(interval);

        //Azkaban接口调用工具类
        azkabanApiUtil = AzkabanApiUtil.getInstance();

        //Projects信息表（Azkaban内部表）
        azkabanProjectsDao = new AzkabanProjectsDaoImpl();

        return true;
    }

    /**
     * 程序主处理
     * @return true：成功、false：异常
     */
    private boolean execute() {
        String[] prefixes = projectPrefixes.split(",");
        for(String prefix : prefixes) {
            List<RetryInfoEntity> retryList = getRetryInfoList(prefix);
            retryList.sort(Comparator.comparing(RetryInfoEntity::getFlowName));
            if (!retryProcess(retryList)) {
                logger.error("retry error. project prefix=" + prefix);
            } else {
                logger.info("retry success. project prefix=" + prefix);
            }
        }

        return true;
    }

    /**
     * 失败重试处理
     * @param retryList 重试Flow列表
     * @return true：成功、false：异常
     */
    private boolean retryProcess(List<RetryInfoEntity> retryList) {
        List<RetryInfoEntity> batchList = new ArrayList<>();
        int count = 0;

        List<RetryInfoEntity> errorList = new ArrayList<>();
        for (RetryInfoEntity entity : retryList) {
            batchList.add(entity);

            if (batchList.size() < retryBatchNumber) {
                continue;
            }

            batchExecuteFlow(batchList, errorList);
            count += batchList.size();
            logger.info(String.format("execute %d/%d", count, retryList.size()));

            batchList.clear();
            CommonUtil.sleep(retryInterval);
        }

        if (batchList.size() > 0) {
            batchExecuteFlow(batchList, errorList);
            count += batchList.size();
            logger.info(String.format("execute %d/%d", count, retryList.size()));
        }

        if (errorList.size() > 0) {
            for (RetryInfoEntity entity : errorList) {
                logger.error("retry error. entity=" + entity.toString() + "\n");
            }
            return false;
        }

        return true;
    }

    /**
     * 释放相关资源
     */
    private void close() {
        //Projects信息表（Azkaban内部表）
        azkabanProjectsDao.close();
    }

    /**
     * 获取失败Flow列表
     * @param projectPrefix project前缀
     * @return true：成功、false：异常
     */
    private List<RetryInfoEntity> getRetryInfoList(String projectPrefix) {
        return azkabanProjectsDao.selectRetryFlowInfo(projectPrefix);
    }

    /**
     * 批量调用azkaban flow任务
     * @param retryList 任务列表
     */
    private void batchExecuteFlow(List<RetryInfoEntity> retryList, List<RetryInfoEntity> errorList) {
        for (RetryInfoEntity entity : retryList) {
            if (!azkabanApiUtil.executeFlow(entity.getProjectName(), entity.getFlowName())) {
                errorList.add(entity);
            }
        }
    }
}
