package com.smallelephant.datamodul.processmonitor;

import com.smallelephant.common.entity.DataDictionaryPojo;
import com.smallelephant.common.entity.JDBCConnectionPojo;
import com.smallelephant.common.entity.RetMongoPojo;
import com.smallelephant.common.jdbcutil.DataSourceUtil;
import com.smallelephant.common.xmlutil.XMLReader;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;

public class MongoTaskMonitor {

    private ArrayList<RetMongoPojo> list = new ArrayList<>();

    public ArrayList<RetMongoPojo> getList() {
        return list;
    }

    public ArrayList<RetMongoPojo> getExceptionList() {
        ArrayList<RetMongoPojo> exceptionList = new ArrayList<RetMongoPojo>();
        for (RetMongoPojo retMongoPojo : list) {
            if (retMongoPojo.getStatus() == 1) {
                exceptionList.add(retMongoPojo);
            }
        }
        return exceptionList;
    }

    public ArrayList<RetMongoPojo> getSuccessList() {
        ArrayList<RetMongoPojo> successList = new ArrayList<RetMongoPojo>();
        for (RetMongoPojo retMongoPojo : list) {
            if (retMongoPojo.getStatus() == 0) {
                successList.add(retMongoPojo);
            }
        }
        return successList;
    }


    public Boolean executeSelect(String xmlFilePath, String dataDictionaryTag) {

        SimpleDateFormat logSdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        final String JDBCConnectionPojo = "com.smallelephant.common.entity.JDBCConnectionPojo";
        JDBCConnectionPojo dataDictionaryConnectionPojo = (JDBCConnectionPojo) XMLReader.getXMInfo(JDBCConnectionPojo, xmlFilePath, dataDictionaryTag, 1);
        System.out.println(logSdf.format(new Date()) + " MongoTaskMonitor " + "INFO " + MongoTaskMonitor.class + " dataDictionaryJDBCPOJO连接信息" + dataDictionaryConnectionPojo.toString());
        Connection connection;


        /***
         * 查询Azkaban库下的元数据表，获取今日发生DDL更新的表数据,获取最末尾执行的任务开始时间，减掉系统时间得到等待Azkaban任务执行时间的等待时长
         */
        connection = DataSourceUtil.getConnection(dataDictionaryConnectionPojo);
        StringBuilder sqlStatementQuery = new StringBuilder();
        sqlStatementQuery.append("SELECT ret.ret_id as id,\n" +
                "ret.ret_syn_start_time as start_time,\n" +
                "ret.ret_syn_end_time as end_time,\n" +
                "TIMESTAMPDIFF(SECOND,ret.ret_syn_start_time,ret.ret_syn_end_time) as time_diff,\n" +
                "ret.ret_count as count,\n" +
                "ret.ret_syn_type as type,\n" +
                "ret.ret_syn_status as status,\n" +
                "et.et_name as tname \n" +
                "FROM `recoding_etl_table` ret left join etl_tables et on ret.ret_et_id = et.et_id \n" +
                "where DATE_FORMAT(ret.ret_syn_start_time,'%Y-%m-%d') = CURRENT_DATE()");
        PreparedStatement pst = null;
        ResultSet resultSet = null;
        try {
            pst = connection.prepareStatement(sqlStatementQuery.toString());
            resultSet = pst.executeQuery();
            RetMongoPojo retMongoPojo;
            while (resultSet.next()) {
                retMongoPojo = new RetMongoPojo();
                retMongoPojo.setId(resultSet.getString("id"));
                retMongoPojo.setTname(resultSet.getString("tname"));
                retMongoPojo.setCount(resultSet.getInt("count"));
                retMongoPojo.setStart_time(resultSet.getString("start_time"));
                retMongoPojo.setEnd_time(resultSet.getString("end_time"));
                retMongoPojo.setTime_diff(resultSet.getString("time_diff"));
                retMongoPojo.setType(resultSet.getString("type"));
                retMongoPojo.setStatus(resultSet.getInt("status"));
                list.add(retMongoPojo);
            }
            System.out.println(logSdf.format(new Date()) + " AzkabanTaskMonitor " + "INFO " + MongoTaskMonitor.class + " Azkaban任务数据查询完毕,共：" + list.size() + "条； " + sqlStatementQuery.toString());
            return true;
        } catch (SQLException e) {
            System.out.println(logSdf.format(new Date()) + " AzkabanTaskMonitor " + "ERROR " + MongoTaskMonitor.class + " SQL异常：" + sqlStatementQuery.toString() + e.getMessage());
            return  false;
        } finally {
            DataSourceUtil.closeAll(resultSet, pst, connection);
        }


    }
}
