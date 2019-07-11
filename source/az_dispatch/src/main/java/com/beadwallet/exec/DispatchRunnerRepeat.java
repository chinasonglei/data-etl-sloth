package com.beadwallet.exec;

import com.beadwallet.bean.AutoGenerationInfo;
import com.beadwallet.cons.Constants;
import com.beadwallet.dao.AzkabanProjectsDao;
import com.beadwallet.dao.CronDictionaryDao;
import com.beadwallet.dao.DataDictionaryDao;
import com.beadwallet.dao.MetaExecuteDao;
import com.beadwallet.dao.entity.AzkabanProjectsEntity;
import com.beadwallet.dao.entity.CronDictionaryEntity;
import com.beadwallet.dao.entity.DataDictionaryEntity;
import com.beadwallet.dao.entity.DispatchFlowInfoEntity;
import com.beadwallet.dao.entity.MetaExecuteEntity;
import com.beadwallet.dao.impl.AzkabanProjectsDaoImpl;
import com.beadwallet.dao.impl.CronDictionaryDaoImpl;
import com.beadwallet.dao.impl.DataDictionaryDaoImpl;
import com.beadwallet.dao.impl.MetaExecuteDaoImpl;
import com.beadwallet.utils.azkaban.AzkabanApiUtil;
import com.beadwallet.utils.common.CommonUtil;
import com.beadwallet.utils.common.EtlFlowComparator;
import com.beadwallet.utils.config.ConfigUtil;
import com.beadwallet.utils.file.FileUtil;
import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Azkaban任务调度程序.
 *
 * @author QuChunhui 2019/01/28
 */
public class DispatchRunnerRepeat {
    private static final Logger logger = LoggerFactory.getLogger(DispatchRunnerRepeat.class);
    private AzkabanApiUtil azkabanApiUtil = null;
    private AzkabanProjectsDao azkabanProjectsDao = null;
    private CronDictionaryDao cronDictionaryDao = null;
    private DataDictionaryDao dataDictionaryDao = null;
    private MetaExecuteDao metaExecuteDao = null;
    private String businessDate = null;
    private String etlFolder = null;
    private String etlPrefix = null;
    private String autoGenePath = null;
    private String dispatchTier = null;
    private String batchNumberStr = null;
    private String firstHoursStr = null;
    private String firstMinuteStr = null;

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

        DispatchRunnerRepeat runner = new DispatchRunnerRepeat(args[0]);

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
    DispatchRunnerRepeat(String configPath) {
        ConfigUtil configUtil = ConfigUtil.getInstance();
        configUtil.loadProperties(configPath);
    }

    /**
     * Class全局变量初始化.
     */
    public boolean initialize() {
        //ETL工程文件所在路径
        etlFolder = ConfigUtil.getProperties(ConfigUtil.ETL_SCRIPT_FOLDER);
        if (etlFolder == null || etlFolder.isEmpty()) {
            logger.error(String.format(
                "property %s is not exist.", ConfigUtil.ETL_SCRIPT_FOLDER));
            return false;
        }

        //ETL工程的前缀名称
        etlPrefix = ConfigUtil.getProperties(ConfigUtil.ETL_PROJECT_PREFIX);
        if (etlPrefix == null) {
            logger.error(String.format(
                "property %s is not exist.", ConfigUtil.ETL_PROJECT_PREFIX));
            return false;
        }

        //自动生成脚本文件所在路径
        autoGenePath = ConfigUtil.getProperties(ConfigUtil.AUTO_GENERATION_PATH);
        if (autoGenePath == null) {
            logger.error(String.format(
                "property %s is not exist.", ConfigUtil.AUTO_GENERATION_PATH));
            return false;
        }

        //ETL调度任务层级范围
        dispatchTier = ConfigUtil.getProperties(ConfigUtil.DISPATCH_TIER);
        if (dispatchTier == null) {
            logger.error(String.format(
                "property %s is not exist.", ConfigUtil.DISPATCH_TIER));
            return false;
        }

        //ETL每批调度的任务数量
        batchNumberStr = ConfigUtil.getProperties(ConfigUtil.ETL_BATCH_NUMBER);
        if (batchNumberStr == null) {
            logger.error(String.format(
                "property %s is not exist.", ConfigUtil.ETL_BATCH_NUMBER));
            return false;
        }

        //ETL调度任务开始时间
        firstHoursStr = ConfigUtil.getProperties(ConfigUtil.ETL_FIRST_HOURS);
        if (firstHoursStr == null) {
            logger.error(String.format(
                "property %s is not exist.", ConfigUtil.ETL_FIRST_HOURS));
            return false;
        }

        //ETL调度任务开始分钟
        firstMinuteStr = ConfigUtil.getProperties(ConfigUtil.ETL_FIRST_MINUTE);
        if (firstMinuteStr == null) {
            logger.error(String.format(
                "property %s is not exist.", ConfigUtil.ETL_FIRST_MINUTE));
            return false;
        }

        //业务日期（yyyyMMdd）
        businessDate = CommonUtil.getBusinessDate();

        //Azkaban接口调用工具类
        azkabanApiUtil = AzkabanApiUtil.getInstance();

        //Projects信息表（Azkaban内部表）
        azkabanProjectsDao = new AzkabanProjectsDaoImpl();

        //元数据字典表Dao
        dataDictionaryDao = new DataDictionaryDaoImpl();

        //Crontab字典表
        cronDictionaryDao = new CronDictionaryDaoImpl();

        //模块执行结果表
        metaExecuteDao = new MetaExecuteDaoImpl();


        return true;
    }

    /**
     * 释放全局变量资源.
     */
    public void close() {
        //Projects信息表（Azkaban内部表）
        azkabanProjectsDao.close();

        //元数据字典表Dao
        dataDictionaryDao.close();

        //Crontab字典表
        metaExecuteDao.close();
        cronDictionaryDao.close();
    }

    /**
     * 业务主处理
     */
    public boolean execute() {
        logger.info("dispatch repeat start.");

        //获取元数据变更信息
        List<DataDictionaryEntity> dicEntityList = getDataDictionaryEntity();
        if (dicEntityList == null || dicEntityList.size() <= 0) {
            logger.info("metadata is no update.");
            updateExecuteStatus(false);
            return false;
        }

        //获取自动生成文件信息
        AutoGenerationInfo autoGeneInfo = getAutoGeneInfo();
        if (autoGeneInfo == null) {
            logger.error("auto generation file error.");
            updateExecuteStatus(false);
            return false;
        }

        //检查zip工程名称前缀
        if (!CommonUtil.checkFilePrefix(autoGeneInfo.getZipFile(), etlPrefix)) {
            logger.error("zip file name error.");
            updateExecuteStatus(false);
            return false;
        }

        //元数据信息与自动生成文件信息整合
        List<DispatchFlowInfoEntity>  flowInfoList = combineDicAndFlow(dicEntityList, autoGeneInfo);
        if (flowInfoList == null || flowInfoList.size() <= 0) {
            logger.error("has inconsistent between dictionary and file.");
            updateExecuteStatus(false);
            return false;
        }

        //更新Azkaban Project
        if (!recreateProject(autoGeneInfo)) {
            logger.error("update azkaban project error.");
            updateExecuteStatus(false);
            return false;
        }

        //更新Azkaban Schedule
        if (!scheduleExecute(flowInfoList)) {
            logger.error("update azkaban schedule error.");
            updateExecuteStatus(false);
            return false;
        }

        //更新模块执行结果
        updateExecuteStatus(true);

        logger.info("dispatch repeat success.");
        return true;
    }

    /**
     * 更新模块运行结果
     */
    private void updateExecuteStatus(boolean status) {
        MetaExecuteEntity entity = new MetaExecuteEntity();
        //模块名称
        entity.setModule("az_dispatch");
        //模块执行状态0：失败 1：成功
        entity.setStatus(status);
        //模块执行时间
        entity.setDate(CommonUtil.convStrToSqlDate(businessDate));

        metaExecuteDao.insert(entity);
        logger.info("inert meta execute. info=" + entity.toString());
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
            if (!etlFolder.equals(name)) {
                continue;
            }

            //获取zip文件
            logger.info("folder path=" + path);
            List<File> zipFileList = new ArrayList<>();
            FileUtil.getFileList(zipFileList, path, Constants.EXPANDED_NAME_ZIP);

            logger.info("zipFileList size=" + zipFileList.size());
            if (zipFileList.size() != 1) {
                logger.error("zip file count error.");
                return null;
            }

            //获取Flow文件
            List<File> flowFileList = new ArrayList<>();
            FileUtil.getFileList(flowFileList, path, Constants.FLOW_FILE_EXPANDED_NAME);

            logger.info("flowFileList size=" + flowFileList.size());
            if (flowFileList.size() <= 0) {
                logger.error("flow file count error.");
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
     * 根据元数据信息，以及自动生成的文件，获取调度任务信息列表.
     *
     * @param dicEntityList 元数据信息列表
     * @param autoGeneInfo 自动生成文件列表
     * @return 数据有不一致问题时返回null。正常时返回调度任务信息列表
     */
    private List<DispatchFlowInfoEntity> combineDicAndFlow(
        List<DataDictionaryEntity> dicEntityList, AutoGenerationInfo autoGeneInfo) {

        List<DispatchFlowInfoEntity> result = new ArrayList<>();

        List<DataDictionaryEntity> errorList1 = new ArrayList<>();
        List<DataDictionaryEntity> errorList2 = new ArrayList<>();
        for (DataDictionaryEntity entity : dicEntityList) {
            //按拼接规则生成Flow名
            List<String> joinFlowNameList = joinFlowName(entity);
            if (joinFlowNameList == null) {
                logger.error("join flow name error.");
                return null;
            }

            for (String joinFlowName : joinFlowNameList) {
                //检查是否存在实际文件
                File flowFile = getFlowFile(autoGeneInfo, joinFlowName);

                //文件不一致条件
                if (entity.isLoad2hive() && flowFile == null) {
                    errorList1.add(entity);
                    continue;
                }
                if (!entity.isLoad2hive() && flowFile != null) {
                    errorList2.add(entity);
                    continue;
                }

                //生成调度信息
                DispatchFlowInfoEntity flowInfo =
                    makeFlowInfoEntity(entity, autoGeneInfo.getZipFile(), joinFlowName);
                if (flowInfo == null) {
                    logger.error("make flow info error.");
                    return null;
                }

                result.add(flowInfo);
            }
        }

        //打印错误日志
        if (errorList1.size() > 0 || errorList2.size() > 0) {
            for (DataDictionaryEntity entity : errorList1) {
                logger.error("flow file is no exist. entity=" + entity.toString());
            }
            for (DataDictionaryEntity entity : errorList2) {
                logger.error("flow file is unneed. entity=" + entity.toString());
            }
            logger.error("no exist count=" + errorList1.size());
            logger.error("unneed count=" + errorList2.size());
            return null;
        }

        logger.info("check metadata and file success.");
        return result;
    }

    /**
     * 生成调度任务信息
     *
     * @param entity 元数据信息
     * @param zipFile zip文件
     * @param flowName flow名
     * @return 调度任务信息
     */
    private DispatchFlowInfoEntity makeFlowInfoEntity(
        DataDictionaryEntity entity, File zipFile, String flowName) {
        if (zipFile == null || flowName == null) {
            logger.error("zip file or flow file is null");
            return null;
        }

        //获得Azkaban Project名称
        String projectName = CommonUtil.getFileNameNoEx(zipFile.getName());

        DispatchFlowInfoEntity flowInfo = new DispatchFlowInfoEntity();
        //project类型
        flowInfo.setProject_type(etlPrefix);
        //flow名
        flowInfo.setFlow_name(CommonUtil.getFileNameNoEx(flowName));
        //project名
        flowInfo.setProject_name(projectName);
        //业务名
        flowInfo.setBusiness_source(entity.getBusiness_source());
        //数据源
        flowInfo.setDb_source(entity.getDb_source());
        //数据库名
        flowInfo.setDb_name(entity.getDb_name());
        //表名
        flowInfo.setTable(entity.getTable_name());
        //数据总大小
        flowInfo.setData_length(entity.getData_length());
        //数据增量
        flowInfo.setIncrease_num(entity.getIncreate_num());
        //偏移量
        flowInfo.setTime_offset(entity.getTime_offset());
        //调度等级
        flowInfo.setLevel(entity.getLevel());
        //逻辑删除标记
        flowInfo.setDelete(false);
        //更新时间
        flowInfo.setLast_update(CommonUtil.convStrToSqlDate(businessDate));
        //是否为DDL
        flowInfo.setCurrent_ddl(entity.isCurrent_ddl());
        //是否为加载对象
        flowInfo.setLoad2hive(entity.isLoad2hive());

        return flowInfo;
    }

//    /**
//     * 检查是否存在文件不一致
//     * 文件不一致条件（OR）
//     * 条件1：load2hive=true，同时文件不存在
//     * 条件2：load2hive=false，同时文件存在
//     *
//     * @param entity 元数据信息表
//     * @param flowFile 文件名
//     * @return true：有异常、false：没有异常
//     */
//    private boolean checkFileDiscordError(DataDictionaryEntity entity, File flowFile) {
//        return entity.isLoad2hive() && flowFile == null || !entity.isLoad2hive() && flowFile != null;
//    }

    /**
     * 获取某个指定的，Flow文件.
     *
     * @param autoGeneInfo 自动生成文件信息
     * @param fileName 文件名
     * @return Flow文件
     */
    private File getFlowFile(AutoGenerationInfo autoGeneInfo, String fileName) {
        for (File file : autoGeneInfo.getFlowFileList()) {
            if (fileName.equals(file.getName())) {
                return file;
            }
        }
        return null;
    }

    /**
     * 根据元数据信息，按照规则获得Flow名称.
     *
     * @param entity 元数据信息
     * @return Flow名称
     */
    private List<String> joinFlowName(DataDictionaryEntity entity) {
        List<String> flowList = new ArrayList<>();
        List<String> tierList = getTierList(entity);

        for (String tier : tierList) {
            String sb = Constants.FLOW_FILE_PREFIX
                + "_"
                + tier
                + "_"
                + entity.getBusiness_source()
                + "_"
                + entity.getDb_name()
                + "_"
                + entity.getTable_name()
                + "_"
                + Constants.FLOW_FILE_EXPANDED_NAME.toLowerCase();

            flowList.add(sb);
        }

        return flowList;
    }

    private List<String> getTierList(DataDictionaryEntity entity) {
        String tier = entity.getTier();

        if (tier != null && !tier.isEmpty()) {
            return Arrays.asList(tier.split(","));
        } else {
            return Arrays.asList(dispatchTier.split(","));
        }
    }

    /**
     * 更新Azkaban Schedule信息.
     *
     * @param flowInfoList Flow文件列表
     * @return true：成功、false：异常
     */
    private boolean scheduleExecute(List<DispatchFlowInfoEntity> flowInfoList) {
        //调度任务排序
        logger.debug(String.format("before=%s", flowInfoList));
        flowInfoList.sort(new EtlFlowComparator());
        logger.debug(String.format("after=%s", flowInfoList));

        //每批次任务个数
        int batchNumber = Integer.valueOf(batchNumberStr);
        int firstHours = Integer.valueOf(firstHoursStr);
        int firstMinute = Integer.valueOf(firstMinuteStr);
        logger.info(String.format("firstHours=%s,"
            + " firstMinute=%s, batchNumber=%s", firstHours, firstMinute, batchNumber));

        //获取Crontab字典
        List<CronDictionaryEntity> cronDictList = getCronDictList();

        int dispatchIndex = 0;
        boolean hasStart = false;
        for (CronDictionaryEntity cronDict : cronDictList) {
            int dictHours = cronDict.getHours();
            int dictMinute = cronDict.getMin();
            logger.debug(String.format("dictHours=%s, dictMinute=%s", dictHours, dictMinute));

            //检查是调度开始时间点
            if (!hasStart) {
                if (dictHours < firstHours || dictMinute < firstMinute) {
                    logger.debug("next cron.");
                    continue;
                }
                hasStart = true;
            }

            //按批次调度任务
            for (int i = 0; i < batchNumber; i++) {
                //所有任务调度完毕
                if (dispatchIndex >= flowInfoList.size()) {
                    logger.info("all dispatch success. dispatchIndex=" + dispatchIndex);
                    return true;
                }

                //获取一个调度任务
                DispatchFlowInfoEntity flowInfo = flowInfoList.get(dispatchIndex);

                //调度时间偏移量
                int timeOffset = flowInfo.getTime_offset();
                if (timeOffset > dictHours) {
                    break;
                }

                //更新调度任务
                if (callScheduleApi(flowInfo, cronDict)) {
                    dispatchIndex++;
                } else {
                    return false;
                }
            }

            logger.info("batch dispatch success. dispatchIndex=" + dispatchIndex);
        }

        return true;
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
                logger.info("find the etl project[" + projectName + "].");
                return projectName;
            }
        }

        logger.info("can't find the etl project from azkaban.");
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
        String activeProjectName = getActiveProjectName(etlPrefix);

        //删除旧工程
        if (activeProjectName != null && !activeProjectName.isEmpty()) {
            if (!azkabanApiUtil.deleteProject(activeProjectName)) {
                logger.error("delete etl project error.");
                return false;
            }
        }

        //创建新工程
        String newProjectName = CommonUtil.getFileNameNoEx(autoGeneInfo.getZipFile().getName());
        if (!azkabanApiUtil.createProject(newProjectName)) {
            logger.error("create etl project error.");
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
     * 调用Azkaban Schedule API
     *
     * @param flowInfo 调度任务
     * @param cronDict cron字典
     * @return true：成功、false：异常
     */
    private boolean callScheduleApi(DispatchFlowInfoEntity flowInfo, CronDictionaryEntity cronDict) {
        //Project名称
        String projectName = flowInfo.getProject_name();
        //Flow名称
        String flowName = flowInfo.getFlow_name();
        //调度时间
        String cronStr = cronDict.getCron_str();

        logger.debug(String.format(
            "project=%s, flow=%s, cron=%s", projectName, flowName, cronStr));
        return azkabanApiUtil.scheduleByCronEXEaFlow(projectName, flowName, cronStr);
    }

    /**
     * 获取Crontab信息字典表.
     *
     * @return Crontab信息列表
     */
    private List<CronDictionaryEntity> getCronDictList() {
        return cronDictionaryDao.selectCronDicInfo();
    }

    /**
     * 获取元数据信息列表.
     *
     * @return 元数据信息列表
     */
    private List<DataDictionaryEntity> getDataDictionaryEntity() {
        Object[] param = new Object[]{CommonUtil.convStrToSqlDate(businessDate)};
        return dataDictionaryDao.selectDictionaryInfo(param);
    }
}
