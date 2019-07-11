package com.beadwallet.exec;

import com.beadwallet.dao.AzkabanProjectsDao;
import com.beadwallet.dao.entity.ActiveExecutingFlowsEntity;
import com.beadwallet.dao.impl.AzkabanProjectsDaoImpl;
import com.beadwallet.utils.azkaban.AzkabanApiUtil;
import com.beadwallet.utils.config.ConfigUtil;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DispatchCancelFlow {
    private static final Logger logger = LoggerFactory.getLogger(DispatchCancelFlow.class);
    private AzkabanProjectsDao azkabanProjectsDao = null;
    private AzkabanApiUtil azkabanApiUtil = null;

    private DispatchCancelFlow(String configPath) {
        ConfigUtil configUtil = ConfigUtil.getInstance();
        configUtil.loadProperties(configPath);
    }

    /**
     * 程序入口函数
     * @param args 参数1：配置文件路径。
     *              参数2：前缀列表（逗号分隔）
     */
    public static void main(String[] args) {
        if (args.length < 1) {
            return;
        }

        //对象初始化
        DispatchCancelFlow retry = new DispatchCancelFlow(args[0]);

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
        //Azkaban接口调用工具类
        azkabanApiUtil = AzkabanApiUtil.getInstance();

        //Projects信息表（Azkaban内部表）
        azkabanProjectsDao = new AzkabanProjectsDaoImpl();

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
     * 程序主处理
     * @return true：成功、false：异常
     */
    private boolean execute() {
        List<ActiveExecutingFlowsEntity> execIdList =  getPreparingExecList();

        int index = 1;
        for (ActiveExecutingFlowsEntity entity : execIdList) {
            String execId = entity.getExecId();
            boolean result = azkabanApiUtil.cancelEXEaFlow(execId);
            logger.info(String.format("current=%s, result=%s, progress=%s/%s",
                execId, result, index++, execIdList.size()));
        }

        return true;
    }

    /**
     * 获取Preparing列表
     * @return true：成功、false：异常
     */
    private List<ActiveExecutingFlowsEntity> getPreparingExecList() {
        return azkabanProjectsDao.selectPreparingExecId();
    }
}
