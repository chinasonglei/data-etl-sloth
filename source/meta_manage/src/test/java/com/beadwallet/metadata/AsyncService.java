package com.beadwallet.metadata;

import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

/**
 * @ClassName AsyncService
 * @Description
 * @Author kai wu
 * @Date 2019/2/28 11:04
 * @Version 1.0
 */
@Service
public class AsyncService {
    @Async(value = "asyncServiceExecutor")
    public void executorAsyncTask(Integer i)
    {
        System.out.println("执行异步：" + i);
    }


    @Async(value = "asyncServiceExecutor")
    public void executorAsyncTaskPlus(Integer i)
    {
        System.out.println("执行异步任务+1: " + (i+1));
    }
}
