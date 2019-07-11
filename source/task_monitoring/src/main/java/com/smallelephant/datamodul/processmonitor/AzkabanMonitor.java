package com.smallelephant.datamodul.processmonitor;

import com.smallelephant.common.entity.AzkabanProjectPojo;
import com.smallelephant.common.entity.JDBCConnectionPojo;
import com.smallelephant.common.jdbcutil.DataSourceUtil;
import com.smallelephant.common.xmlutil.XMLReader;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;

/***
 * 监控Azkaban的任务情况
 */
public class AzkabanMonitor {

    public Long executeUpdate(String xmlFilePath, String azkabanTag) {

        SimpleDateFormat logSdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        final String JDBCConnectionPojo = "com.smallelephant.common.entity.JDBCConnectionPojo";
        JDBCConnectionPojo azkabanConnectionPojo = (JDBCConnectionPojo) XMLReader.getXMInfo(JDBCConnectionPojo, xmlFilePath, azkabanTag, 1);
        System.out.println(logSdf.format(new Date()) + " DataManageMonitor " + "INFO " + AzkabanMonitor.class + " azkabanJDBCPOJO连接信息" + azkabanConnectionPojo.toString());
        Connection connection;


        /***
         * 查询Azkaban库下的元数据表，获取今日发生DDL更新的表数据,获取最末尾执行的任务开始时间，减掉系统时间得到等待Azkaban任务执行时间的等待时长
         */
        connection = DataSourceUtil.getConnection(azkabanConnectionPojo);
        StringBuilder sqlStatementQuery = new StringBuilder();
        sqlStatementQuery.append("select A.id, B.flow_id, B.start_time ");
        sqlStatementQuery.append("from projects A, execution_flows B ");
        sqlStatementQuery.append("where A.id = B.project_id and A.active = 1 and A.`name` LIKE 'data_etl%' and 1 = 1 ");
        sqlStatementQuery.append("order by B.start_time DESC ");
        PreparedStatement pst = null;
        ResultSet resultSet = null;
        ArrayList<AzkabanProjectPojo> list = new ArrayList<AzkabanProjectPojo>();
        try {
            pst = connection.prepareStatement(sqlStatementQuery.toString());
            resultSet = pst.executeQuery();
            AzkabanProjectPojo azkabanProjectPojo;
            while (resultSet.next()) {
                azkabanProjectPojo = new AzkabanProjectPojo();
                azkabanProjectPojo.setId(resultSet.getInt("id"));
                azkabanProjectPojo.setFlowId(resultSet.getString("flow_id"));
                azkabanProjectPojo.setStartTime(resultSet.getLong("start_time"));
                list.add(azkabanProjectPojo);
            }
            System.out.println(logSdf.format(new Date()) + " DataManageMonitor " + "INFO " + AzkabanMonitor.class + " Azkaban元数据查询完毕,共：" + list.size() + "条； " + sqlStatementQuery.toString());
        } catch (SQLException e) {
            System.out.println(logSdf.format(new Date()) + " DataManageMonitor " + "ERROR " + AzkabanMonitor.class + " SQL异常：" + sqlStatementQuery.toString() + e.getMessage());
        } finally {
            DataSourceUtil.closeAll(resultSet, pst, connection);
        }

        if (list.size() < 1) {
            System.out.println(logSdf.format(new Date()) + " DataManageMonitor " + "WARN " + AzkabanMonitor.class + " Azkaban-Project-Flows数量异常!!!可能是没有有效的 data_rtlXXXXX Project或者该项目下无flow");
            return (long) (-1);
        } else {
            /**
             *用毫秒值% (24 * 60 * 60 * 1000) 得到当日的0点起的毫秒值，才有比较的价值
             */
            System.out.println(logSdf.format(new Date()) + " DataManageMonitor " + "INFO " + AzkabanMonitor.class + " Azkaban-Project-Flows末位执行开始时间:" + logSdf.format(list.get(0).getStartTime()));
            return list.get(0).getStartTime() % (24 * 60 * 60 * 1000) - System.currentTimeMillis() % (24 * 60 * 60 * 1000);
        }
    }

}
