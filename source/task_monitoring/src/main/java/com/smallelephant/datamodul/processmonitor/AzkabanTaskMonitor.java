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

public class AzkabanTaskMonitor {

    private ArrayList<String> list = new ArrayList<String>();

    public ArrayList<String> getList() {
        return list;
    }

    public Boolean executeSelect(String xmlFilePath, String azkabanTag) {

        SimpleDateFormat logSdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        final String JDBCConnectionPojo = "com.smallelephant.common.entity.JDBCConnectionPojo";
        com.smallelephant.common.entity.JDBCConnectionPojo azkabanConnectionPojo = (JDBCConnectionPojo) XMLReader.getXMInfo(JDBCConnectionPojo, xmlFilePath, azkabanTag, 1);
        System.out.println(logSdf.format(new Date()) + " AzkabanTaskMonitor " + "INFO " + AzkabanTaskMonitor.class + " azkabanJDBCPOJO连接信息" + azkabanConnectionPojo.toString());
        Connection connection;


        /***
         * 查询Azkaban库下的元数据表，获取今日发生DDL更新的表数据,获取最末尾执行的任务开始时间，减掉系统时间得到等待Azkaban任务执行时间的等待时长
         */
        connection = DataSourceUtil.getConnection(azkabanConnectionPojo);
        StringBuilder sqlStatementQuery = new StringBuilder();
        sqlStatementQuery.append("SELECT \n" +
                "CASE \n" +
                "\tWHEN LEFT(t.`flow_id`,5) = 'alter'  THEN\n" +
                "\t\t\"alter\"\n" +
                "\t\tELSE LEFT(t.`flow_id`,7)\n" +
                "END AS\n" +
                " flow_type,\n" +
                "CASE\n" +
                "\t\n" +
                "\tWHEN f.`status` = 50 THEN\n" +
                "\t\"Success\" \n" +
                "\tWHEN f.`status` = 20 THEN\n" +
                "\t\"Preparing\" \n" +
                "\tWHEN f.`status` = 30 THEN\n" +
                "\t\"Running\" \n" +
                "\telse \"Exception\" \n" +
                "\tEND AS job_status,\n" +
                "\tcount( 1 ) AS sum \n" +
                "FROM\n" +
                "\t(\n" +
                "\tSELECT\n" +
                "\t\tt2.flow_id AS flow_id,\n" +
                "\t\tMAX( t2.exec_id ) AS exec_id \n" +
                "\tFROM\n" +
                "\t\tprojects t1\n" +
                "\t\tINNER JOIN execution_flows t2 ON t2.project_id = t1.id \n" +
                "\t\tAND t1.active = 1 \n" +
                "\t\tWHERE t1.`name` LIKE 'data_etl_20%' \n" +
                "\t\tor t1.`name` LIKE 'data_once%' \n" +
                "\tand TO_DAYS(FROM_UNIXTIME(t2.submit_time/1000,'%y-%m-%d')) = TO_DAYS(NOW()) \n" +
                "\tGROUP BY\n" +
                "\t\tflow_id \n" +
                "\t) t\n" +
                "\tINNER JOIN execution_flows f ON t.exec_id = f.exec_id \n" +
                "GROUP BY\n" +
                "\tflow_type,\n" +
                "\tf.`status` \n" +
                "ORDER BY\n" +
                "\tflow_type ASC,\n" +
                "job_status DESC;");
        PreparedStatement pst = null;
        ResultSet resultSet = null;
        try {
            pst = connection.prepareStatement(sqlStatementQuery.toString());
            resultSet = pst.executeQuery();
            AzkabanProjectPojo azkabanProjectPojo;
            while (resultSet.next()) {
                String flow_type = resultSet.getString("flow_type");
                String status = resultSet.getString("job_status");
                int sum = resultSet.getInt("sum");
                list.add(flow_type + " : " + status + " : " + sum);
            }
            System.out.println(logSdf.format(new Date()) + " AzkabanTaskMonitor " + "INFO " + AzkabanTaskMonitor.class + " Azkaban任务数据查询完毕,共：" + list.size() + "条； " + sqlStatementQuery.toString());
            return true;
        } catch (SQLException e) {
            System.out.println(logSdf.format(new Date()) + " AzkabanTaskMonitor " + "ERROR " + AzkabanTaskMonitor.class + " SQL异常：" + sqlStatementQuery.toString() + e.getMessage());
            return  false;
        } finally {
            DataSourceUtil.closeAll(resultSet, pst, connection);
        }


    }
}
