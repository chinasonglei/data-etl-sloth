package com.beadwallet.metadata.service;

/**
 * @ClassName PkIndexOffsetService
 * @Description
 * @Author kai wu
 * @Date 2019/3/21 15:16
 * @Version 1.0
 */
public interface PkIndexOffsetService {

    /**
     *  该方法用于查询hive数仓中按主键进行etl业务表的最大主键值
     *
     * @Date  2019/3/21 18:16
     * @Param [xmlPath]
     * @return boolean
     **/
    boolean pkIndexOffsetCount(String xmlPath);
}
