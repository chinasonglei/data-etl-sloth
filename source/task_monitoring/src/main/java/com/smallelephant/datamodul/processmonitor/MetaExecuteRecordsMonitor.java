package com.smallelephant.datamodul.processmonitor;

import com.smallelephant.common.entity.JDBCConnectionPojo;
import com.smallelephant.common.jdbcutil.DataSourceUtil;
import com.smallelephant.common.xmlutil.XMLReader;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.text.SimpleDateFormat;
import java.util.Date;

/***
 * 元数据执行记录状态监控
 */
public class MetaExecuteRecordsMonitor {

    public boolean executeMonitor(String xmlFilePath, String metaExecuteRecordsTag) {

        SimpleDateFormat logSdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        final String JDBCConnectionPojo = "com.smallelephant.common.entity.JDBCConnectionPojo";
        JDBCConnectionPojo dataDictionaryConnectionPojo = (JDBCConnectionPojo) XMLReader.getXMInfo(JDBCConnectionPojo, xmlFilePath, metaExecuteRecordsTag, 1);
        Connection connection;
        System.out.println(logSdf.format(new Date()) + " DataManageMonitor " + "INFO " + MetaExecuteRecordsMonitor.class + " XML读取dataDictionaryConnectionPojo信息" + dataDictionaryConnectionPojo.toString());

        connection = DataSourceUtil.getConnection(dataDictionaryConnectionPojo);
        if (null == connection) {
            System.out.println(logSdf.format(new Date()) + " DataManageMonitor " + "ERROR " + MetaExecuteRecordsMonitor.class + " 查询data_dictionary表，获取JDBC连接失败");
            return false;
        }
        StringBuilder sqlStatementQuery = new StringBuilder();
        sqlStatementQuery.append("select status from meta_execute_records ");
        sqlStatementQuery.append("where module = 'az_dispatch' ");
        sqlStatementQuery.append("and date = ? ;");
        PreparedStatement pst = null;
        ResultSet resultSet = null;
        boolean status = false;
        try {
            SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
            String date = simpleDateFormat.format(new Date());
            pst = connection.prepareStatement(sqlStatementQuery.toString());
            pst.setString(1, date);
            resultSet = pst.executeQuery();
            while (resultSet.next()) {
                status = resultSet.getInt("status") == 1;
                if (status)
                    System.out.println(logSdf.format(new Date()) + " DataManageMonitor " + "INFO " + MetaExecuteRecordsMonitor.class + " 查询meta_execute_records验证azkaban模块执行完毕状态 " + sqlStatementQuery);
            }
        } catch (Exception e) {
            System.out.println(logSdf.format(new Date()) + " DataManageMonitor " + "ERROR " + MetaExecuteRecordsMonitor.class + " 查询meta_execute_records验证azkaban模块执行完毕状态 " + sqlStatementQuery + e.getMessage());
            return false;
        } finally {
            DataSourceUtil.closeAll(resultSet, pst, connection);
        }
        return status;
    }
}
