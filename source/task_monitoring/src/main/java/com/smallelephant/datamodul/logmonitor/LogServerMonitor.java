package com.smallelephant.datamodul.logmonitor;

import com.smallelephant.common.emailutil.MailUtil;
import com.smallelephant.common.entity.MailReceiverPojo;
import com.smallelephant.common.entity.MailSenderPojo;
import com.smallelephant.datamodul.logserver.LogServer;
import com.smallelephant.common.xmlutil.XMLReader;

import java.io.File;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class LogServerMonitor {

    public Boolean sendExceptionEmail(String xmlFilePath, String logFile, int exceptionLogsNumThreshold, int waitTimeSecondsThreshold) {

        final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        SimpleDateFormat logSdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        final String classNameForMailSenderPojo = "com.smallelephant.common.entity.MailSenderPojo";
        final String classNameForMailReceiverPojo = "com.smallelephant.common.entity.MailReceiverPojo";
        final LogServer logServer = new LogServer();
        final MailReceiverPojo mailReceiver = (MailReceiverPojo) XMLReader.getXMInfo(classNameForMailReceiverPojo, xmlFilePath, "MailReceiver", 1);
        final MailSenderPojo mailSender = (MailSenderPojo) XMLReader.getXMInfo(classNameForMailSenderPojo, xmlFilePath, "MailSender", 1);
        System.out.println(logSdf.format(new Date()) + " DataManageMonitor " + "INFO " + LogServerMonitor.class + " 邮件发送人信息" + mailSender.toString());
        System.out.println(logSdf.format(new Date()) + " DataManageMonitor " + "INFO " + LogServerMonitor.class + " 邮件接收人信息" + mailReceiver.toString());

        /**
         * 启动日志监控功能，并等待日志监控完成后发送异常信息邮件
         */
        File log = new File(logFile);
        MailUtil.getInstance(mailSender);
        mailReceiver.setSubject("--监控日志" + sdf.format(new Date()));

        logServer.logParse(log);
        logServer.setWaitTimeSecondThreshold(waitTimeSecondsThreshold);
        logServer.setLogsNumsThreshold(exceptionLogsNumThreshold);
        final ScheduledExecutorService service = Executors.newScheduledThreadPool(1);
        service.scheduleWithFixedDelay(new Runnable() {
            public void run() {
                if (!logServer.isRunning()) {
                        mailReceiver.setContent("<center><h2><font face=\"verdana\">数仓自动化调度系统告警信息</font></h2></center>");
                        System.out.println(logSdf.format(new Date()) + " DataManageMonitor " + "INFO " + LogServerMonitor.class + " 日志监控逻辑--正常结束，日志信息，准备发送邮件");
                        mailReceiver.setContent("<h3><font face=\"verdana\">异常日志共：" + logServer.getExceptionLogsList().size() + "条</font></h3>");
                        if (logServer.getExceptionLogsList().size() >= 1) {
                            mailReceiver.setContent(logServer.getExceptionLogsList().toString());
                        }
                    service.shutdown();
                }
            }
        }, 0, 3, TimeUnit.SECONDS);
        //service没有停止前，后续任务等待
        int i = 0;
        while (!service.isShutdown()) {
            try {
                System.out.println(logSdf.format(new Date()) + " DataManageMonitor " + "INFO " + LogServerMonitor.class + " 等待日志监控完成中..." + 10 * i++ + "s");
                Thread.sleep(10000);
            } catch (InterruptedException e) {
                System.out.println(logSdf.format(new Date()) + " DataManageMonitor " + "ERROR " + LogServerMonitor.class + " 等待日志监控失败" + e.getMessage());
                return false;
            }
        }
        //发送异常日志的邮件报告
        Boolean status = true;
        if (mailReceiver.getContent().length() >= 10)
            status = MailUtil.sendEmail(mailReceiver);
        if (!status) {
            return false;
        } else {
            return true;
        }
    }
}
