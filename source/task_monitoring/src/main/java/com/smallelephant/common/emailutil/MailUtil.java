package com.smallelephant.common.emailutil;

import com.smallelephant.common.entity.MailReceiverPojo;
import com.smallelephant.common.entity.MailSenderPojo;
import org.apache.commons.mail.EmailAttachment;
import org.apache.commons.mail.EmailException;
import org.apache.commons.mail.HtmlEmail;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

/**
 * editor: lzy
 * last modify: 2019-01-14
 */
public class MailUtil {

    private static String mailServerHost;
    private static String mailSenderAddress;
    private static String mailSenderNick;
    private static String mailSenderUsername;
    private static String mailSenderPassword;


    public static MailUtil getInstance(MailSenderPojo mailSenderInfo) {
        mailServerHost = mailSenderInfo.getMailServerHost();
        mailSenderAddress = mailSenderInfo.getMailSenderAddress();
        mailSenderNick = mailSenderInfo.getMailSenderNick();
        mailSenderUsername = mailSenderInfo.getMailSenderUsername();
        mailSenderPassword = mailSenderInfo.getMailSenderPassword();
        return new MailUtil();
    }

    /**
     * Send Email
     *
     * @return void
     */
    public static Boolean sendEmail(MailReceiverPojo mailInfo) {

        SimpleDateFormat logSdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        try {
            HtmlEmail email = new HtmlEmail();
            // 配置信息

            email.setHostName(mailServerHost);
            email.setFrom(mailSenderAddress, mailSenderNick);
            email.setAuthentication(mailSenderUsername, mailSenderPassword);
            email.setSocketTimeout(100000);
            email.setCharset("UTF-8");
            email.isStartTLSEnabled();
            email.setSubject(mailInfo.getSubject());
            email.setHtmlMsg(mailInfo.getContent());
            email.setSmtpPort(25);


            // 添加附件
            List<EmailAttachment> attachments = mailInfo.getAttachments();
            if (null != attachments && attachments.size() > 0) {
                for (int i = 0; i < attachments.size(); i++) {
                    email.attach(attachments.get(i));
                }
            }

            // 收件人
            List<String> toAddress = mailInfo.getToAddress();
            if (null != toAddress && toAddress.size() > 0) {
                for (int i = 0; i < toAddress.size(); i++) {
                    email.addTo(toAddress.get(i));
                }
            }
            // 抄送人
            List<String> ccAddress = mailInfo.getCcAddress();
            if (null != ccAddress && ccAddress.size() > 0) {
                for (int i = 0; i < ccAddress.size(); i++) {
                    email.addCc(ccAddress.get(i));
                }
            }
            //邮件模板 密送人
            List<String> bccAddress = mailInfo.getBccAddress();
            if (null != bccAddress && bccAddress.size() > 0) {
                for (int i = 0; i < bccAddress.size(); i++) {
                    email.addBcc(ccAddress.get(i));
                }
            }

            email.send();
        } catch (EmailException e) {
            e.printStackTrace();
            System.out.println(logSdf.format(new Date()) + " DataManageMonitor " + "ERROR " + MailUtil.class + " EmailSender执行失败 " + e.getMessage());
            return false;
        }
        System.out.println(logSdf.format(new Date()) + " DataManageMonitor " + "INFO " + MailUtil.class + " EmailSender执行成功");
        return true;
    }
}
