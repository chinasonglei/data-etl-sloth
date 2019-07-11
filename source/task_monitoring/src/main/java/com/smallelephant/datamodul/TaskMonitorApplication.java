package com.smallelephant.datamodul;

import com.smallelephant.common.emailutil.MailUtil;
import com.smallelephant.common.entity.*;
import com.smallelephant.common.xmlutil.XMLReader;
import com.smallelephant.datamodul.logmonitor.DataDictionaryUpdate;
import com.smallelephant.datamodul.logmonitor.LogServerMonitor;
import com.smallelephant.datamodul.logmonitor.DataRecordsUpdate;
import com.smallelephant.datamodul.processmonitor.AzkabanMonitor;
import com.smallelephant.datamodul.processmonitor.AzkabanTaskMonitor;
import com.smallelephant.datamodul.processmonitor.MetaExecuteRecordsMonitor;
import com.smallelephant.datamodul.processmonitor.MongoTaskMonitor;

import java.io.File;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Map;

public class TaskMonitorApplication {
    /***
     * 三个任务：
     * 1、监控日志
     * 2、查询hive元数据库更新data_dictionary表
     * 3、查询hive更新data_update_records表
     * @param args
     */
    public static void main(String[] args) {

        SimpleDateFormat logSdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

        /**
         * 生成日志标准路径
         */
        String xmlFilePath = args[0];
//        String xmlFilePath = "D:/config/config.xml";
        System.out.println(logSdf.format(new Date()) + " DataManageMonitor " + "INFO " + TaskMonitorApplication.class + " XML配置文件路径：" + xmlFilePath);

        /**
         * 涉及到的XML配置文件标签
         */
        final String metastore = "Metastore";
        final String hiveJDBCTag = "HiveJdbc";
        final String azkabanTag = "AzkabanJdbc";
        final String hiveMetastoreTag = "HiveMetastore";
        final String emailReceiverTag = "MailReceiver";
        final String emailSenderTag = "MailSender";
        final String logServerConf = "LogServerConf";

        /**
         * 邮件实体类
         */
        String classNameForMailSenderPojo = "com.smallelephant.common.entity.MailSenderPojo";
        String classNameForMailReceiverPojo = "com.smallelephant.common.entity.MailReceiverPojo";
        MailReceiverPojo mailReceiver = null;
        MailSenderPojo mailSender = (MailSenderPojo) XMLReader.getXMInfo(classNameForMailSenderPojo, xmlFilePath, emailSenderTag, 1);

        /**
         * 监控日志
         */
        String[] tags = {"path", "exceptionLogsNumThreshold", "waitTimeSecondsThreshold"};
        Map<String, String> logServerConfMap = XMLReader.getXMLInfo(xmlFilePath, logServerConf, tags);
        LogServerMonitor logServerMonitor = new LogServerMonitor();
        SimpleDateFormat simpleDateFormatForLog = new SimpleDateFormat("yyyy-MM-dd");
        String logName = simpleDateFormatForLog.format(new Date()) + ".log";
        String logPath = logServerConfMap.get(tags[0]) + File.separator + logName;
        int exceptionLogsNumThreshold = Integer.parseInt(logServerConfMap.get(tags[1]));
        int waitTimeSecondsThreshold = Integer.parseInt(logServerConfMap.get(tags[2]));
        System.out.println(logSdf.format(new Date()) + " DataManageMonitor " + "INFO " + TaskMonitorApplication.class + " logPath" + logPath);
        if (logServerMonitor.sendExceptionEmail(xmlFilePath, logPath, exceptionLogsNumThreshold, waitTimeSecondsThreshold)) {
            System.out.println(logSdf.format(new Date()) + " DataManageMonitor " + "INFO " + TaskMonitorApplication.class + " 日志监控成功");
        } else {
            System.out.println(logSdf.format(new Date()) + " DataManageMonitor " + "WARN " + TaskMonitorApplication.class + " 发送异常邮件失败");
            return;
        }


        /**
         * 监控MetaExecuteRecordsMonitor表中任务的执行状态,执行后续任务
         */
        MetaExecuteRecordsMonitor metaExecuteRecordsMonitor = new MetaExecuteRecordsMonitor();
        while (!metaExecuteRecordsMonitor.executeMonitor(xmlFilePath, metastore)) {
            try {
                Thread.sleep(1000 * 60);
                System.out.println(logSdf.format(new Date()) + " DataManageMonitor " + "INFO " + TaskMonitorApplication.class + " 监控MetaExecuteRecordsMonitor表->az_diapatch状态：未完成；");
            } catch (InterruptedException e) {
                System.out.println(logSdf.format(new Date()) + " DataManageMonitor " + "WARN " + TaskMonitorApplication.class + " 监控MetaExecuteRecordsMonitor表->线程等待出现异常" + e.getMessage());
                return;
            }
        }


        /**
         * 查询azkaban表，获取当日最后执行的任务时间，等待至该时刻执行后续任务
         */
        /*Long waitTime = (long) (1000 * 300);//增加的等待时常300s
        AzkabanMonitor azkabanMonitor = new AzkabanMonitor();
        Long azkabanWaitTime = azkabanMonitor.executeUpdate(xmlFilePath, azkabanTag);
        try {
            System.out.println(logSdf.format(new Date()) + " DataManageMonitor " + "INFO " + TaskMonitorApplication.class + " 等待azkaban任务执行完毕中,等待时间为：" + (azkabanWaitTime + waitTime) / 1000);
            if (azkabanWaitTime >= 0) {
                Thread.sleep(azkabanWaitTime + waitTime);
            } else {
                Thread.sleep(0);
            }
            System.out.println(logSdf.format(new Date()) + " DataManageMonitor " + "INFO " + TaskMonitorApplication.class + " azkaban任务基本执行完毕...");
        } catch (InterruptedException e) {
            System.out.println(logSdf.format(new Date()) + " DataManageMonitor " + "WARN " + TaskMonitorApplication.class + " 等待azkaban任务执行完毕出现异常" + e.getMessage());
            return;
        }*/

        /**
         * 查询hive元数据库更新data_dictionary表
         */
        DataDictionaryUpdate dataDictionaryUpdate = new DataDictionaryUpdate();
        if (dataDictionaryUpdate.executeUpdate(xmlFilePath, metastore, hiveMetastoreTag)) {
            System.out.println(logSdf.format(new Date()) + " DataManageMonitor " + "INFO " + TaskMonitorApplication.class + " 查询hive元数据库更新data_dictionary表成功...");
        } else {
            System.out.println(logSdf.format(new Date()) + " DataManageMonitor " + "WARN " + TaskMonitorApplication.class + " 查询hive元数据库更新data_dictionary表失败");
            return;
        }


        /**
         * 将data_dictionary表中的更新状态信息邮件通知
         */

        MailUtil.getInstance(mailSender);
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        mailReceiver = (MailReceiverPojo) XMLReader.getXMInfo(classNameForMailReceiverPojo, xmlFilePath, emailReceiverTag, 1);
        mailReceiver.setSubject("--入库状态" + sdf.format(new Date()));
        mailReceiver.setContent("<center><h2><font face=\"verdana\">数仓自动化ODS/RDS层DDL操作状态信息</font></h2></center>");
        /**
         * data_dictionary表中的更新状态
         */
        ArrayList<DataDictionaryPojo> dataDictionaryExceptionList = dataDictionaryUpdate.getExceptionList();
        mailReceiver.setContent("<h3><font face=\"verdana\">DDL状态信息：" + dataDictionaryUpdate.getList().size() + "条, 异常状态信息：" + dataDictionaryExceptionList.size() + "条</font></h3>");

        ArrayList<DataDictionaryPojo> dataDictionaryList = dataDictionaryUpdate.getList();
        DataDictionaryPojo dataDictionaryPojo = new DataDictionaryPojo();
        if(dataDictionaryList.size()>0){
            mailReceiver.setContent(dataDictionaryPojo.sendEmailTitle());
            for (DataDictionaryPojo dataDictionaryPojo2 : dataDictionaryList) {
//                if (dataDictionaryExceptionList.indexOf(dataDictionaryPojo) == 0 ||dataDictionaryExceptionList.size() ==0)
//                    mailReceiver.setContent(dataDictionaryPojo.sendEmailTitle());
                mailReceiver.setContent(dataDictionaryPojo2.sendEmailMessage());
            }
        }

        mailReceiver.setContent("</table>");
        System.out.println(logSdf.format(new Date()) + " DataManageMonitor " + "INFO " + TaskMonitorApplication.class + " 将data_dictionary的更新状态信息收集，准备发送邮件通知更新信息");
        MailUtil.sendEmail(mailReceiver);
        mailReceiver = null;


        /**
         * 查询Azkaban上任务执行的情况，发送邮件通知;
         */
        MailUtil.getInstance(mailSender);
        mailReceiver = (MailReceiverPojo) XMLReader.getXMInfo(classNameForMailReceiverPojo, xmlFilePath, emailReceiverTag, 1);
        mailReceiver.setSubject("--入库状态" + sdf.format(new Date()));
        mailReceiver.setContent("<center><h2><font face=\"verdana\">数仓自动化ODS/RDS层数据入库状态信息</font></h2></center>");
        AzkabanTaskMonitor azkabanTaskMonitor = new AzkabanTaskMonitor();
        Boolean azkabanTaskMonitorStatus = azkabanTaskMonitor.executeSelect(xmlFilePath, azkabanTag);
        if (azkabanTaskMonitorStatus) {
            ArrayList<String> list = azkabanTaskMonitor.getList();
            for (String s : list) {
                mailReceiver.setContent(s+"<br>");
            }
        }
        mailReceiver.setContent("</table>");
        System.out.println(logSdf.format(new Date()) + " DataManageMonitor " + "INFO " + TaskMonitorApplication.class + " 查询azkaban task，准备发送邮件通知更新信息");
        MailUtil.sendEmail(mailReceiver);
        mailReceiver = null;

        /**
         * 查询MongoDB数据recording_etl_table表的情况，发送邮件通知；
         */
        MongoTaskMonitor mongoTaskMonitor = new MongoTaskMonitor();
        Boolean mongoTaskMonitorStatus = mongoTaskMonitor.executeSelect(xmlFilePath, metastore);
        MailUtil.getInstance(mailSender);
        mailReceiver = (MailReceiverPojo) XMLReader.getXMInfo(classNameForMailReceiverPojo, xmlFilePath, emailReceiverTag, 1);
        mailReceiver.setSubject("--入库状态" + sdf.format(new Date()));
        mailReceiver.setContent("<center><h2><font face=\"verdana\">data_etl_bridge入库状态信息报告</font></h2></center>");

        SimpleDateFormat sb = new SimpleDateFormat("yyyy年MM月dd日 HH时mm分ss秒");
        String time = sb.format(new Date());
        ArrayList<RetMongoPojo> RetMongoExceptionList = mongoTaskMonitor.getExceptionList();
        mailReceiver.setContent("<h3><font face=\"verdana\">1.时间："+time+"</font></h3>");
        mailReceiver.setContent("<h3><font face=\"verdana\">2.概览：总量：" + mongoTaskMonitor.getList().size() + "条, 成功：" + mongoTaskMonitor.getSuccessList().size() + "条,失败："+RetMongoExceptionList.size()+"条</font></h3>");
        if(mongoTaskMonitor.getList().size()>0){
            mailReceiver.setContent("<h3><font face=\"verdana\">3.明细：</font></h3>");
        }


        if (mongoTaskMonitorStatus) {
            ArrayList<RetMongoPojo> list = mongoTaskMonitor.getList();
            if(list.size()>0) {
                mailReceiver.setContent(RetMongoPojo.sendEmailTitle());
                for (RetMongoPojo RetMongoPojo : list) {
                    mailReceiver.setContent(RetMongoPojo.sendEmailMessage());
                }
            }
        }
        mailReceiver.setContent("</table></center>");
        System.out.println(logSdf.format(new Date()) + " DataManageMonitor " + "INFO " + TaskMonitorApplication.class + " 查询azkaban task，准备发送邮件通知更新信息");
        MailUtil.sendEmail(mailReceiver);




        /**
         * 查询hive更新data_update_records表
         */
/*        DataRecordsUpdate dataRecordsUpdate = new DataRecordsUpdate();
        if (dataRecordsUpdate.executeUpdate(xmlFilePath, metastore, hiveJDBCTag)) {
            System.out.println(logSdf.format(new Date()) + " DataManageMonitor " + "INFO " + TaskMonitorApplication.class + " 查询hive更新data_update_records表成功...");
        } else {
            System.out.println(logSdf.format(new Date()) + " DataManageMonitor " + "WARN " + TaskMonitorApplication.class + " 查询hive更新data_update_records表失败");
            return;
        }*/


        /**
         * 将data_update_records表中的更新状态信息邮件通知
         */
/*        MailUtil.getInstance(mailSender);
        mailReceiver = (MailReceiverPojo) XMLReader.getXMInfo(classNameForMailReceiverPojo, xmlFilePath, emailReceiverTag, 1);
        mailReceiver.setSubject("--入库状态" + sdf.format(new Date()));
        mailReceiver.setContent("<center><h2><font face=\"verdana\">数仓自动化ODS/RDS层数据入库状态信息</font></h2></center>");*/
        /**
         * data_update_records表中的更新状态
         */
        /*ArrayList<DataRecordsUpdatePojo> dataRecordsExceptionList = dataRecordsUpdate.getExceptionList();
        mailReceiver.setContent("<h3><font face=\"verdana\">入库状态信息：" + dataRecordsUpdate.getList().size() + "条， 异常状态信息：" + dataRecordsExceptionList.size() + "条</font></h3>");
        for (DataRecordsUpdatePojo dataRecordsUpdatePojo : dataRecordsExceptionList) {
            if (dataRecordsExceptionList.indexOf(dataRecordsUpdatePojo) == 0)
                mailReceiver.setContent(dataRecordsUpdatePojo.sendEmailTitle());
            mailReceiver.setContent(dataRecordsUpdatePojo.sendEmailMessage());
        }*/
        /*mailReceiver.setContent("</table>");
        System.out.println(logSdf.format(new Date()) + " DataManageMonitor " + "INFO " + TaskMonitorApplication.class + " 将data_update_records表中的更新状态信息收集，准备发送邮件通知更新信息");
        MailUtil.sendEmail(mailReceiver);*/

        System.out.println(logSdf.format(new Date()) + " DataManageMonitor " + "INFO " + TaskMonitorApplication.class + " DataManageMonitor模块全部执行完成...");
    }


}
