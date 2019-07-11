package com.beadwallet.metadata;

import com.beadwallet.common.utils.util.CommonUtil;
import com.beadwallet.metadata.service.MetadataService;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;


@SpringBootTest
@RunWith(SpringRunner.class)
public class MetadataApplicationTests {

    Logger logger = LoggerFactory.getLogger(MetadataApplicationTests.class);

    @Autowired
    private AsyncService asyncService;

    @Autowired
    private MetadataService metadataService;


    /**
     * @Author  kai wu
     * @Description 该方法用于测试元数据同步功能
     * @Date  2019/1/25 14:38
     * @Param []
     * @return void
     **/
    @Test
    public void testMetaData() {
        String xmlPath = "/opt/platform/etl_dispatch/conf/config.xml";
        metadataService.updateMetadata(xmlPath);
    }




    /**
     * @Author  kai wu
     * @Description 该方法用于测试异步方法
     **/
    @Test
    public void testAsync(){

        for(int i = 0; i<10; i++) {
            asyncService.executorAsyncTask(i);
            asyncService.executorAsyncTaskPlus(i);
        }

        for(int j=0;j<10;j++){
            System.out.println("testAsync"+j);
        }
    }

    @Test
    public void ASCIIToConvert() throws Exception{
        String value = "\u007F父机构ID#";

        System.out.println("old:"+value);

        String s = CommonUtil.convertIllegalCharacter(value);

        System.out.println("new:"+ CommonUtil.ClobToString(CommonUtil.stringToClob(s)));
    }

}

