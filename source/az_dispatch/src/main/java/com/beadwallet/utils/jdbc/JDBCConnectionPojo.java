package com.beadwallet.utils.jdbc;

/**
 * JDBC连接对象
 *
 * @author QuChunhui 2019/01/28
 */
public class JDBCConnectionPojo {
    private String comment;
    private String driver;
    private String url;
    private String user;
    private String passwd;

    public JDBCConnectionPojo() {
    }

    public JDBCConnectionPojo(String comment, String driver, String url, String user, String passwd) {
        this.comment = comment;
        this.driver = driver;
        this.url = url;
        this.user = user;
        this.passwd = passwd;
    }

    public String getComment() {
        return comment;
    }

    public void setComment(String comment) {
        this.comment = comment;
    }

    public String getDriver() {
        return driver;
    }

    public void setDriver(String driver) {
        this.driver = driver;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
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

    @Override
    public String toString() {
        return String.format(
            "JDBCConnectionPojo{comment=%s, driver=%s, url=%s, user=%s, passwd=%s}",
            comment, driver, url, user, passwd);
    }
}