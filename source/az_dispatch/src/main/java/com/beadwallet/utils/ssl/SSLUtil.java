package com.beadwallet.utils.ssl;

import com.beadwallet.utils.config.ConfigUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.*;
import java.io.IOException;
import java.net.URL;
import java.security.cert.X509Certificate;

/**
 * 绕过AzkabanSSL验证Class
 *
 * @author QuChunhui 2019/01/28
 */
public class SSLUtil {
    private static final Logger logger = LoggerFactory.getLogger(SSLUtil.class);
    private SSLUtil() {}

    /**
     * 绕过SSL验证
     */
    public static void disableChecks() {
        //获取Azkaban访问地址
        String azkaban_url = ConfigUtil.getProperties(ConfigUtil.AZKABAN_URL);
        if (azkaban_url == null) {
            logger.error(String.format("property %s is not exist.", ConfigUtil.AZKABAN_URL));
            return;
        }

        try {
            new URL(azkaban_url).getContent();
        } catch (IOException e) {
            // This invocation will always fail, but it will register the
            // default SSL provider to the URL class.
        }

        //关闭SSL验证
        try {
            SSLContext sslContext = SSLContext.getInstance("TLS");
            TrustManager[] trustManagerArray = {new AzTrustManager()};
            sslContext.init(null, trustManagerArray, null);
            HttpsURLConnection.setDefaultSSLSocketFactory(sslContext.getSocketFactory());
            HttpsURLConnection.setDefaultHostnameVerifier((s, sslSession) -> true);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * TODO 后续增加不绕过SSL的处理
     */
    private static class AzTrustManager implements X509TrustManager {
        @Override
        public void checkClientTrusted(X509Certificate[] chain, String authType) {
        }

        @Override
        public void checkServerTrusted(X509Certificate[] chain, String authType) {
        }

        @Override
        public X509Certificate[] getAcceptedIssuers() {
            return new X509Certificate[0];
        }
    }
}