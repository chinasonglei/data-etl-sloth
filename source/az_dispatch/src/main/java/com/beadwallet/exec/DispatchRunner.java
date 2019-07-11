package com.beadwallet.exec;

/**
 * Azkaban任务调度程序.
 *
 * @author QuChunhui 2019/01/28
 */
public class DispatchRunner {
    /**
     * 应用启动函数.
     *
     * @param args 需要传递配置文件路径（包括文件名）
     */
    public static void main(String[] args) {
        String configPath = args[0];

        //元数据更新处理
        executeOnce(configPath);

        //数据ETL处理
        executeRepeat(configPath);
    }

    /**
     * 执行元数据更新处理
     *
     * @param configPath 配置文件路径
     */
    private static void executeOnce(String configPath) {
        DispatchRunnerOnce once = new DispatchRunnerOnce(configPath);

        //初始化全局变量
        if (!once.initialize()) {
            once.close();
            return;
        }

        //程序主处理
        if (!once.execute()) {
            once.close();
            return;
        }

        //释放全局资源
        once.close();
    }

    /**
     * 数据ETL处理
     *
     * @param configPath 配置文件路径
     */
    private static void executeRepeat(String configPath) {
        DispatchRunnerRepeat repeat = new DispatchRunnerRepeat(configPath);

        //初始化全局变量
        if (!repeat.initialize()) {
            repeat.close();
            return;
        }

        //程序主处理
        if (!repeat.execute()) {
            repeat.close();
            return;
        }

        //释放全局资源
        repeat.close();
    }
}