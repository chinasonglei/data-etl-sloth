package com.beadwallet.metadata.service.impl;

import com.beadwallet.metadata.service.PkIndexOffsetService;
import com.beadwallet.metadata.service.ScheduledFutureService;
import org.apache.commons.lang3.concurrent.BasicThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.Calendar;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;


/**
 * @ClassName ScheduledFutureServiceImpl
 * @Description
 * @Author kai wu
 * @Date 2019/3/21 18:24
 * @Version 1.0
 */
@Service
public class ScheduledFutureServiceImpl implements ScheduledFutureService {

    Logger logger = LoggerFactory.getLogger(ScheduledFutureServiceImpl.class);

    @Autowired
    private PkIndexOffsetService pkIndexOffsetService;


    /**
     *  定时任务
     *
     * @Date  2019/3/21 18:16
     * @Param []
     **/
    @Override
    public void scheduledFuture(String xmlPath) {
        //定时任务线程池初始化
        ScheduledExecutorService service = new ScheduledThreadPoolExecutor(2, new BasicThreadFactory.Builder().namingPattern("example-schedule-pool-%d").daemon(true).build());

        //1.开启查询hive的定时任务
        Calendar calendar = Calendar.getInstance();
        int year = calendar.get(Calendar.YEAR);
        int month = calendar.get(Calendar.MONTH);
        int day = calendar.get(Calendar.DAY_OF_MONTH);
        calendar.set(year,month,day,19,0,0);
        long targetTimeMillis = calendar.getTimeInMillis();
        long currentTimeMillis = System.currentTimeMillis();
        service.scheduleAtFixedRate(() -> {
            pkIndexOffsetService.pkIndexOffsetCount(xmlPath); }, targetTimeMillis-currentTimeMillis, 24*60*60*1000, TimeUnit.MILLISECONDS);

    }
}
