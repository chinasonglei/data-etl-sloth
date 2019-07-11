package com.beadwallet.metadata.service;

/**
 * @ClassName MetadataService
 * @Description
 * @Author kai wu
 * @Date 2019/1/21 13:42
 * @Version 1.0
 */
public interface MetadataService {


    /**
     * update metadata
     * @param xmlPath
     * @return boolean
     */
    boolean updateMetadata(String xmlPath);
}
