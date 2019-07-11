package com.smallelephant.common.entity;

import org.apache.commons.mail.EmailAttachment;

import java.io.Serializable;
import java.util.List;

/**
 * editor: lzy
 * last modify: 2019-01-14
 */
public class MailReceiverPojo implements Serializable {
    // 收件人
    private List<String> toAddress = null;
    // 抄送人地址
    private List<String> ccAddress = null;
    // 密送人
    private List<String> bccAddress = null;
    // 附件信息
    private List<EmailAttachment> attachments = null;
    // 邮件主题
    private String subject = "";
    // 邮件的文本内容
    private String content = "";

    //重写数据类型为list的成员变量的set方法
    public void setToAddress(String toAddress) {
        this.toAddress.add(toAddress);
    }

    public void setCcAddress(String ccAddress) {
        this.ccAddress.add(ccAddress);
    }

    public void setBccAddress(String bccAddress) {
        this.bccAddress.add(bccAddress);
    }

    public void setAttachments(EmailAttachment attachments) {
        this.attachments.add(attachments);
    }

    //getter and setter
    public List<String> getToAddress() {
        return toAddress;
    }

    public List<String> getCcAddress() {
        return ccAddress;
    }

    public List<String> getBccAddress() {
        return bccAddress;
    }

    public List<EmailAttachment> getAttachments() {
        return attachments;
    }

    public String getSubject() {
        return subject;
    }

    public void setSubject(String subject) {
        this.subject += subject;
    }

    public String getContent() {
        return content;
    }

    public void setContent(String content) {
        this.content += content;
    }

    @Override
    public String toString() {
        return "MailReceiverInfo{" +
                "toAddress=" + toAddress +
                ", ccAddress=" + ccAddress +
                ", bccAddress=" + bccAddress +
                ", attachments=" + attachments +
                ", subject='" + subject + '\'' +
                ", content='" + content + '\'' +
                '}';
    }
}
