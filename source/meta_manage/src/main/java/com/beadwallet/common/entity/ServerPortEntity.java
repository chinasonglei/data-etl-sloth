package com.beadwallet.common.entity;

import java.io.Serializable;

/**
 * @ClassName ServerPortEntity
 * @Description
 * @Author kai wu
 * @Date 2019/2/20 16:02
 * @Version 1.0
 */
public class ServerPortEntity implements Serializable {
    private String serverName;
    private String port;

    public ServerPortEntity() {
    }

    public String getServerName() {
        return serverName;
    }

    public void setServerName(String serverName) {
        this.serverName = serverName;
    }

    public String getPort() {
        return port;
    }

    public void setPort(String port) {
        this.port = port;
    }
}
