package com.beadwallet.utils.common;

import java.io.File;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

/**
 * 模块内通用类
 *
 * @author QuChunhui 2019/01/28
 */
public class CommonUtil {
    /**
     * 取得业务日期
     * @return yyyyMMdd
     */
    public static String getBusinessDate() {
        Calendar cal = Calendar.getInstance();
        cal.setTimeInMillis(System.currentTimeMillis());
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
        return sdf.format(cal.getTime());
    }

    /**
     * 日期转换为SQL Date格式
     * @param yyyyMMdd 日期
     * @return java.sql.date
     */
    public static java.sql.Date convStrToSqlDate(String yyyyMMdd) {
        try {
            SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
            Date dt2 = sdf.parse(yyyyMMdd);
            return new java.sql.Date(dt2.getTime());
        } catch (Exception ignored) {
            return null;
        }
    }

    /**
     * 获取不带扩展名的文件名
     * @param filename 文件名称（包含扩展名）
     * @return 文件名
     */
    public static String getFileNameNoEx(String filename) {
        if (filename != null && filename.length() > 0) {
            int dot = filename.lastIndexOf('.');
            if (dot > -1 && dot < filename.length()) {
                return filename.substring(0, dot);
            }
        }
        return filename;
    }

    /**
     * 睡眠指定的时间
     * @param millisecond 毫秒
     */
    public static void sleep(int millisecond) {
        try {
            Thread.sleep(millisecond);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    /**
     * 检查文件是否以指定前缀开头
     * @param file 文件
     * @param prefix 前缀
     * @return true：是、false：否
     */
    public static boolean checkFilePrefix(File file, String prefix) {
        return file != null && file.getName().startsWith(prefix);
    }

    /**
     * 检查ResultSet中是否有指定列
     * @param rs ResultSet
     * @param columnName 列明
     * @return true：存在、false：不存在
     */
    public static boolean isExistColumn(ResultSet rs, String columnName) {
        try {
            if (rs.findColumn(columnName) > 0 ) {
                return true;
            }
        } catch (SQLException e) {
            return false;
        }

        return false;
    }
}