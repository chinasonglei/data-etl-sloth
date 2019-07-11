package com.beadwallet.exec;

import com.beadwallet.bean.AutoGenerationInfo;
import com.beadwallet.cons.Constants;
import com.beadwallet.dao.AzkabanProjectsDao;
import com.beadwallet.dao.entity.AzkabanProjectsEntity;
import com.beadwallet.dao.impl.AzkabanProjectsDaoImpl;
import com.beadwallet.utils.azkaban.AzkabanApiUtil;
import com.beadwallet.utils.common.CommonUtil;
import com.beadwallet.utils.common.PrepareFlowComparator;
import com.beadwallet.utils.config.ConfigUtil;
import com.beadwallet.utils.file.FileUtil;
import java.io.File;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Azkaban任务调度程序.
 *
 * @author QuChunhui 2019/01/28
 */
public class DispatchRunnerOnce {
    private static final Logger logger = LoggerFactory.getLogger(DispatchRunnerOnce.class);
    private AzkabanApiUtil azkabanApiUtil = null;
    private AzkabanProjectsDao azkabanProjectsDao = null;
    private String prepareFolder = null;
    private String preparePrefix = null;
    private String prepareBatchNumber = null;
    private String prepareInterval = null;
    private String autoGenePath = null;

    /**
     * 应用启动函数.
     *
     * @param args 需要传递配置文件路径（包括文件名）
     */
    public static void main(String[] args) {
        if (args == null || args.length != 1) {
            logger.error("parameter error.");
            return;
        }

        DispatchRunnerOnce runner = new DispatchRunnerOnce(args[0]);

        //初始化全局变量
        if (!runner.initialize()) {
            runner.close();
            return;
        }

        //程序主处理
        if (!runner.execute()) {
            runner.close();
            return;
        }

        //释放全局资源
        runner.close();
    }

    /**
     * Constructor.
     *
     * @param configPath 配置文件路径
     */
    DispatchRunnerOnce(String configPath) {
        ConfigUtil configUtil = ConfigUtil.getInstance();
        configUtil.loadProperties(configPath);
    }

    /**
     * Class全局变量初始化.
     */
    public boolean initialize() {
        //ETL元数据更新工程文件所在路径
        prepareFolder = ConfigUtil.getProperties(ConfigUtil.ETL_PREPARE_FOLDER);
        if (prepareFolder == null || prepareFolder.isEmpty()) {
            logger.error(String.format(
                "property %s is not exist.", ConfigUtil.ETL_PREPARE_FOLDER));
            return false;
        }

        //ETL元数据更新工程的前缀名称
        preparePrefix = ConfigUtil.getProperties(ConfigUtil.ETL_PREPARE_PROJECT_PREFIX);
        if (preparePrefix == null) {
            logger.error(String.format(
                "property %s is not exist.", ConfigUtil.ETL_PREPARE_PROJECT_PREFIX));
            return false;
        }

        //ETL元数据更新工程每批次执行数量
        prepareBatchNumber = ConfigUtil.getProperties(ConfigUtil.ETL_PREPARE_BATCH_NUMBER);
        if (prepareBatchNumber == null) {
            logger.error(String.format(
                "property %s is not exist.", ConfigUtil.ETL_PREPARE_BATCH_NUMBER));
            return false;
        }

        //ETL元数据更新工程单位: 毫秒
        prepareInterval = ConfigUtil.getProperties(ConfigUtil.ETL_PREPARE_INTERNAL);
        if (prepareInterval == null) {
            logger.error(String.format(
                "property %s is not exist.", ConfigUtil.ETL_PREPARE_INTERNAL));
            return false;
        }

        //自动生成脚本文件所在路径
        autoGenePath = ConfigUtil.getProperties(ConfigUtil.AUTO_GENERATION_PATH);
        if (autoGenePath == null) {
            logger.error(String.format(
                "property %s is not exist.", ConfigUtil.AUTO_GENERATION_PATH));
            return false;
        }

        //Azkaban接口调用工具类
        azkabanApiUtil = AzkabanApiUtil.getInstance();

        //Projects信息表（Azkaban内部表）
        azkabanProjectsDao = new AzkabanProjectsDaoImpl();

        return true;
    }

    /**
     * 释放全局变量资源.
     */
    public void close() {
        //Projects信息表（Azkaban内部表）
        azkabanProjectsDao.close();
    }

    /**
     * 业务主处理
     *
     * @return true：正常结束、false：不需要继续处理
     */
    public boolean execute() {
        logger.info("dispatch once start.");

        //获取自动生成文件信息
        AutoGenerationInfo autoGeneInfo = getAutoGeneInfo();
        if (autoGeneInfo == null) {
            logger.info("auto generation file null.");
            return false;
        }

        //检查zip工程名称前缀
        if (!CommonUtil.checkFilePrefix(autoGeneInfo.getZipFile(), preparePrefix)) {
            logger.error("zip file name error.");
            return false;
        }

        //更新Azkaban Project
        if (!recreateProject(autoGeneInfo)) {
            logger.error("update azkaban project error.");
            return false;
        }

        //运行Azkaban Flow
        if (!callExecuteFlowApi(autoGeneInfo)) {
            logger.error("update azkaban schedule error.");
            return false;
        }

        logger.info("dispatch once success.");
        return true;
    }

    /**
     * 构建自动生成文件信息对象列表.
     *
     * @return 自动生成文件信息对象列表
     */
    private AutoGenerationInfo getAutoGeneInfo() {
        //文件夹列表
        List<File> folderList = FileUtil.getChildFolderList(autoGenePath);
        logger.info("folderList size=" + folderList.size());

        for (File folder : folderList) {
            //子文件夹路径
            String path = folder.getPath();
            //子文件夹名
            String name = folder.getName();

            //非目标文件夹
            if (!prepareFolder.equals(name)) {
                continue;
            }

            //获取zip文件
            logger.info("folder path=" + path);
            List<File> zipFileList = new ArrayList<>();
            FileUtil.getFileList(zipFileList, path, Constants.EXPANDED_NAME_ZIP);

            logger.info("zipFileList size=" + zipFileList.size());
            if (zipFileList.size() != 1) {
                logger.info("zip file not exist.");
                return null;
            }

            //获取Flow文件
            List<File> flowFileList = new ArrayList<>();
            FileUtil.getFileList(flowFileList, path, Constants.FLOW_FILE_EXPANDED_NAME);

            logger.info("flowFileList size=" + flowFileList.size());
            if (flowFileList.size() <= 0) {
                logger.info("flow file not exist.");
                return null;
            }

            //构建自动生成对象
            AutoGenerationInfo autoGeneInfo = new AutoGenerationInfo();
            autoGeneInfo.setFolder(folder);
            autoGeneInfo.setZipFile(zipFileList.get(0));
            autoGeneInfo.setFlowFileList(flowFileList);
            return autoGeneInfo;
        }

        return null;
    }

    /**
     * 从Azkaban系统中获取当前的ETL Project名称.
     *
     * @return etl project名称
     */
    private String getActiveProjectName(String projectNamePrefix) {
        //从azkaban内部表中获取所有运行的Project
        List<AzkabanProjectsEntity> entityList = azkabanProjectsDao.selectAzkabanProjects();

        //按照前缀查找ETL Project。
        for (AzkabanProjectsEntity entity : entityList) {
            String projectName = entity.getName();
            if (projectName.startsWith(projectNamePrefix)) {
                logger.info("find the prepare project[" + projectName + "].");
                return projectName;
            }
        }

        logger.info("can't find the prepare project from azkaban.");
        return null;
    }

    /**
     * 重上传Azkaban Project Zip文件.
     *
     * @param autoGeneInfo 工程文件信息
     * @return true：成功、false：异常
     */
    private boolean recreateProject(AutoGenerationInfo autoGeneInfo) {
        //获取正在在运行工程
        String activeProjectName = getActiveProjectName(preparePrefix);

        //删除旧工程
        if (activeProjectName != null && !activeProjectName.isEmpty()) {
            if (!azkabanApiUtil.deleteProject(activeProjectName)) {
                logger.error("delete prepare project error.");
                return false;
            }
        }

        //创建新工程
        String newProjectName = CommonUtil.getFileNameNoEx(autoGeneInfo.getZipFile().getName());
        if (!azkabanApiUtil.createProject(newProjectName)) {
            logger.error("create prepare project error.");
            return false;
        }

        //获取zip文件名
        String zipFilePath = autoGeneInfo.getZipFile().getPath();

        //上传工程文件
        if (!azkabanApiUtil.uploadZip(newProjectName, zipFilePath)) {
            logger.error("upload zip file error.");
            return false;
        }

        logger.info("recreate azkaban project success.");
        return true;
    }

    /**
     * 直接运行Azkaban Flow.
     *
     * @param autoGeneInfo 自动生成文件信息
     */
    private boolean callExecuteFlowApi(AutoGenerationInfo autoGeneInfo) {
        //获取Project名
        String projectName = CommonUtil.getFileNameNoEx(autoGeneInfo.getZipFile().getName());

        //排序：保证建表语句在前
        List<File> flowFileList = autoGeneInfo.getFlowFileList();
        flowFileList.sort(new PrepareFlowComparator());

        //每批次执行数量
        int batchNum = prepareBatchNumber == null ? 1 : Integer.valueOf(prepareBatchNumber);

        //每批次时间间隔
        int internal = prepareInterval == null ? 0 : Integer.valueOf(prepareInterval);

        //循环所有调度任务
        List<File> batchList = new ArrayList<>();
        int count = 0;
        for (File flowFile : flowFileList) {
            batchList.add(flowFile);

            if (batchList.size() < batchNum) {
                continue;
            }

            if (!batchExecuteFlow(projectName, batchList)) {
                return false;
            }

            count += batchList.size();
            logger.info(String.format("execute %d/%d", count, flowFileList.size()));
            batchList.clear();
            CommonUtil.sleep(internal);
        }

        if (batchList.size() > 0) {
            if (!batchExecuteFlow(projectName, batchList)) {
                return false;
            }
            count += batchList.size();
            logger.info(String.format("execute %d/%d", count, flowFileList.size()));
        }

        return true;
    }

    /**
     * 批量调用azkaban flow任务
     * @param projectName project名
     * @param flowList 任务列表
     * @return true：成功、false：异常
     */
    private boolean batchExecuteFlow(String projectName, List<File> flowList) {
        for (File flow : flowList) {
            //直接执行azkaban
            if (!azkabanApiUtil.executeFlow(
                projectName, CommonUtil.getFileNameNoEx(flow.getName()))) {
                return false;
            }
        }
        return true;
    }
}