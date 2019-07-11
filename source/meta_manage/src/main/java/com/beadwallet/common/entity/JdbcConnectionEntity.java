package com.beadwallet.common.entity;

import java.io.Serializable;

/**
 * @ClassName JdbcConnectionEntity
 * @Description 数据源配置对象类
 * @Author kai wu
 * @Date 2019/1/10 10:32
 * @Version 1.0
 */
public class JdbcConnectionEntity implements Serializable {
    /**
     * 数据库信息
     */
    private String comment;

    /**
     * 业务名称
     */
    private String businessSource;

    /**
     * 数据库名称
     */
    private String dbName;

    /**
     * 数据库url
     */
    private String url;
    /**
     * 数据库driver
     */
    private String driver;
    /**
     * 数据库username
     */
    private String user;
    /**
     * 数据库password
     */
    private String passwd;

    public String getComment() {
        return comment;
    }

    public void setComment(String comment) {
        this.comment = comment;
    }

    public String getBusinessSource() {
        return businessSource;
    }

    public void setBusinessSource(String businessSource) {
        this.businessSource = businessSource;
    }

    public String getDbName() {
        return dbName;
    }

    public void setDbName(String dbName) {
        this.dbName = dbName;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public String getDriver() {
        return driver;
    }

    public void setDriver(String driver) {
        this.driver = driver;
    }

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public String getPasswd() {
        return passwd;
    }

    public void setPasswd(String passwd) {
        this.passwd = passwd;
    }

    public JdbcConnectionEntity() {
    }


}
