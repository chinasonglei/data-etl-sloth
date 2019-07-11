package com.beadwallet.metadata;

import com.beadwallet.common.entity.ServerPortEntity;
import com.beadwallet.common.utils.xmlutil.XMLReader;
import com.beadwallet.metadata.service.MetadataService;
import com.beadwallet.metadata.service.ScheduledFutureService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.web.server.ConfigurableWebServerFactory;
import org.springframework.boot.web.server.WebServerFactory;
import org.springframework.boot.web.server.WebServerFactoryCustomizer;
import org.springframework.context.annotation.ComponentScan;

import java.util.ArrayList;

/**
 * @ClassName MetadataApplication
 * @Description
 * @Author kai wu
 * @Date 2019/1/9 9:22
 * @Version 1.0
 */
@SpringBootApplication
@ComponentScan(basePackages = {"com.beadwallet"})
public class MetadataApplication implements CommandLineRunner , WebServerFactoryCustomizer {

	static Logger logger = LoggerFactory.getLogger(MetadataApplication.class);

	static String xmlPath;

	@Value("${config.server.port}")
	private String serverPort;

	@Value("${server.port.entity}")
	private String serverPortEntity;

	@Autowired
	private MetadataService metadataService;


	@Autowired
	private ScheduledFutureService scheduledFutureService;


	/**
	 * @Description 程序入口
	 * @Date  2019/2/11 12:10
	 **/
	public static void main(String[] args) {
		if(args==null || args.length==0){
			logger.error("args[] is null,can't find arg in it,please check");
		}else{
			xmlPath = args[0];
			logger.info("xmlConfig file path is {}",xmlPath);
		}
		SpringApplication.run(MetadataApplication.class,args);
	}

	/**
	 * @Description 程序的端口号设置
	 * @Date  2019/2/11 12:10
	 **/
    @Override
    public void customize(WebServerFactory factory) {
        //用于测试
//		xmlPath = "/opt/platform/etl_dispatch/conf/config.xml";

        if(xmlPath!=null){
            ConfigurableWebServerFactory configurableWebServerFactory =  (ConfigurableWebServerFactory)factory;
            ArrayList<ServerPortEntity> ports = XMLReader.getXMInfo(serverPortEntity, xmlPath, serverPort);
            if(ports.size()==0){
                logger.error("serverPort is null,please check it");
            }else{
                for (ServerPortEntity serverPortEntity :ports) {
                    if(serverPortEntity !=null && "meta_manage".equals(serverPortEntity.getServerName())){
                        logger.info("serverPort is {}", serverPortEntity.getPort());
                        configurableWebServerFactory.setPort(Integer.parseInt(serverPortEntity.getPort()));
                        break;
                    }
                }
            }
        }else{
            logger.error("xmlPath is null,please check it");
        }

    }


	/**
	 * @Description 元数据更新及增量数据统计入口
	 * @Date  2019/2/11 12:10
	 **/
	@Override
    public void run(String[] args){
        //用于测试
//		xmlPath = "/opt/platform/etl_dispatch/conf/config.xml";

        if(xmlPath!=null){
            logger.info("xmlPath is {}",xmlPath);
            boolean result = metadataService.updateMetadata(xmlPath);
            if(result){
               scheduledFutureService.scheduledFuture(xmlPath);
            }
        }else{
            logger.error("xmlPath is null,please check it");
        }

	}
}

