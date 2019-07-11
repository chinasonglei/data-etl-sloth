package com.smallelephant.common.entity;

import java.io.Serializable;

/**
 * editor: lzy
 * last modify: 2019-01-14
 * MailSender对象
 */
public class MailSenderPojo implements Serializable {

    private String mailServerHost;
    private String mailSenderAddress;
    private String mailSenderNick;
    private String mailSenderUsername;
    private String mailSenderPassword;


    public MailSenderPojo() {
    }

    public MailSenderPojo(String mailServerHost, String mailSenderAddress, String mailSenderNick, String mailSenderUsername, String mailSenderPassword) {
        this.mailServerHost = mailServerHost;
        this.mailSenderAddress = mailSenderAddress;
        this.mailSenderNick = mailSenderNick;
        this.mailSenderUsername = mailSenderUsername;
        this.mailSenderPassword = mailSenderPassword;
    }

    @Override
    public String toString() {
        return "MailSenderPojo{" +
                "mailServerHost='" + mailServerHost + '\'' +
                ", mailSenderAddress='" + mailSenderAddress + '\'' +
                ", mailSenderNick='" + mailSenderNick + '\'' +
                ", mailSenderUsername='" + mailSenderUsername + '\'' +
                ", mailSenderPassword='" + mailSenderPassword + '\'' +
                '}';
    }

    public String getMailServerHost() {
        return mailServerHost;
    }

    public void setMailServerHost(String mailServerHost) {
        this.mailServerHost = mailServerHost;
    }

    public String getMailSenderAddress() {
        return mailSenderAddress;
    }

    public void setMailSenderAddress(String mailSenderAddress) {
        this.mailSenderAddress = mailSenderAddress;
    }

    public String getMailSenderNick() {
        return mailSenderNick;
    }

    public void setMailSenderNick(String mailSenderNick) {
        this.mailSenderNick = mailSenderNick;
    }

    public String getMailSenderUsername() {
        return mailSenderUsername;
    }

    public void setMailSenderUsername(String mailSenderUsername) {
        this.mailSenderUsername = mailSenderUsername;
    }

    public String getMailSenderPassword() {
        return mailSenderPassword;
    }

    public void setMailSenderPassword(String mailSenderPassword) {
        this.mailSenderPassword = mailSenderPassword;
    }
}
