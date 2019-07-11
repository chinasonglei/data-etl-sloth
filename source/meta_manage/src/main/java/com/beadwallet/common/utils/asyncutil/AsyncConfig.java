package com.beadwallet.common.utils.asyncutil;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.AsyncConfigurer;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import java.util.concurrent.Executor;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * @ClassName AsyncConfig
 * @Description 线程池配置类
 * @Author kai wu
 * @Date 2019/1/9 9:22
 * @Version 1.0
 */
@Configuration
@EnableAsync
public class AsyncConfig {
    Logger logger = LoggerFactory.getLogger(AsyncConfig.class);

    @Value("${async.executor.thread.core_pool_size}")
    private String core;

    @Value("${async.executor.thread.max_pool_size}")
    private String max;

    @Value("${async.executor.thread.queue_capacity}")
    private String queue;

    @Value("${async.executor.thread.name.prefix}")
    private String prefix;

    @Bean
    public Executor asyncServiceExecutor() {
        VisibleThreadPoolTaskExecutor executor = new VisibleThreadPoolTaskExecutor();
        //配置核心线程数
        executor.setCorePoolSize(Integer.parseInt(core));
        //配置最大线程数
        executor.setMaxPoolSize(Integer.parseInt(max));
        //配置队列大小
        executor.setQueueCapacity(Integer.parseInt(queue));
        //配置线程池中的线程的名称前缀
        executor.setThreadNamePrefix(prefix);
        // 设置拒绝策略
        executor.setRejectedExecutionHandler(new ThreadPoolExecutor.CallerRunsPolicy());
        // 等待所有任务结束后再关闭线程池
        executor.setWaitForTasksToCompleteOnShutdown(true);
        return executor;
    }

}
