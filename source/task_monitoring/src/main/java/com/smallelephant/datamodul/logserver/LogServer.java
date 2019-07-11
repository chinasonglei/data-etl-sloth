package com.smallelephant.datamodul.logserver;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class LogServer {
    //日志解析时长
    private static int waitTimeSecond = 0;
    //日志读取偏移
    private static long lastReadFileSize = 0;
    //异常日志容器
    public static ArrayList<String> exceptionLogsList = new ArrayList<String>();
    //异常日志保存条数 阈值
    public static int logsNumsThreshold = 100;//default
    //无日志解析动作后等待时长 阈值
    public static int waitTimeSecondsThreshold = 300;//default
    //日志解析服务运行状态
    private static boolean running = true;

    public void setWaitTimeSecondThreshold(int waitTimeSecondThreshold) {
        LogServer.waitTimeSecondsThreshold = waitTimeSecondThreshold;
    }

    public static void setLogsNumsThreshold(int logsNumsThreshold) {
        LogServer.logsNumsThreshold = logsNumsThreshold;
    }

    public boolean isRunning() {
        return running;
    }

    public ArrayList<String> getExceptionLogsList() {
        return exceptionLogsList;
    }

    /***
     * 解析日志，将异常日志保存到本地
     * @param logFile
     * @throws FileNotFoundException
     */
    public void logParse(File logFile) {
        SimpleDateFormat logSdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

        final RandomAccessFile randomfile;
        if (!logFile.exists()) {
            try {
                logFile.createNewFile();
                System.out.println(logSdf.format(new Date()) + " DataManageMonitor " + "INFO " + LogServer.class + "日志文件不存在，创建成功");
                randomfile = new RandomAccessFile(logFile, "rw");
            } catch (Exception e) {
                System.out.println(logSdf.format(new Date()) + " DataManageMonitor " + "ERROR " + LogServer.class + "日志文件不存在，创建失败 " + e.getMessage());
                return;
            }
        } else {
            try {
                randomfile = new RandomAccessFile(logFile, "rw");
            } catch (FileNotFoundException e) {
                e.printStackTrace();
                return;
            }
        }

        final ScheduledExecutorService executorService = Executors.newScheduledThreadPool(1);
        executorService.scheduleWithFixedDelay(new Runnable() {
            public void run() {
                try {
                    //随机到该位置
                    randomfile.seek(lastReadFileSize);
                    //读取
                    String line;
                    if ((line = randomfile.readLine()) != null) {
                        System.out.println(logSdf.format(new Date()) + " DataManageMonitor " + "INFO " + LogServer.class + " 捕获正常日志：" + line);
                        String exceptionLog = checkLogDetail(line);
                        if (exceptionLog != null) {
                            exceptionLogsList.add("<p><font size=\"4\" face=\"arial\" color=\"red\">" + exceptionLog + "</font>" + "<br>");
                            System.out.println(logSdf.format(new Date()) + " DataManageMonitor " + "INFO " + LogServer.class + " 捕获异常日志信息：" + exceptionLog);
                        }
                        lastReadFileSize += line.length() + "\n".length();
                        waitTimeSecond = 0;
                    } else {
                        waitTimeSecond += 1;
                        //waitTimeSecondThreshold秒无日志写入则将其停掉
                        if (waitTimeSecond / 100 >= waitTimeSecondsThreshold) {
                            running = false;
                            System.out.println(logSdf.format(new Date()) + " DataManageMonitor " + "INFO " + LogServer.class + " LogServer解析结束，" + waitTimeSecondsThreshold + "秒无日志写入-->停止线程");
                            executorService.shutdown();
                        }
                    }
                    //异常日志达到阈值则停止线程，
                    if (exceptionLogsList.size() >= logsNumsThreshold) {
                        running = false;
                        System.out.println(logSdf.format(new Date()) + " DataManageMonitor " + "INFO " + LogServer.class + " LogServer解析结束，异常日志达到阈值-->停止线程");
                        executorService.shutdown();
                    }
                } catch (IOException e) {
                    running = false;
                    System.out.println(logSdf.format(new Date()) + " DataManageMonitor " + "ERROR " + LogServer.class + " LogServer解析异常" + e.getMessage());
                }
            }
        }, 0, 10, TimeUnit.MILLISECONDS);

    }

    private String checkLogDetail(String line) {
        if (null != line) {
            /**
             * 添加乱码日志处理
             */
            if ((line.contains("Exception") || line.contains("WARN") || line.contains("ERROR") && (line.contains("datamanager") || line.contains("etl_script") || line.contains("az_dispath")))) {
                return line;
            }
        }
        return null;
    }


}
