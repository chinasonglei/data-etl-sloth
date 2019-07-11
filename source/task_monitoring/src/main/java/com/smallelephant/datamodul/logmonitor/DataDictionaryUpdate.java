package com.smallelephant.datamodul.logmonitor;

import com.smallelephant.common.entity.DataDictionaryPojo;
import com.smallelephant.common.entity.JDBCConnectionPojo;
import com.smallelephant.common.jdbcutil.DataSourceUtil;
import com.smallelephant.common.xmlutil.XMLReader;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;

/***
 * 监控Hive元数据，更新字典表
 */
public class DataDictionaryUpdate {

    private ArrayList<DataDictionaryPojo> list = new ArrayList<DataDictionaryPojo>();

    public ArrayList<DataDictionaryPojo> getExceptionList() {
        ArrayList<DataDictionaryPojo> exceptionList = new ArrayList<DataDictionaryPojo>();
        for (DataDictionaryPojo dataDictionaryPojo : list) {
            if ((dataDictionaryPojo.getCurrentDdl() == 1 && dataDictionaryPojo.getOds_success() == 0) || (dataDictionaryPojo.getCurrentDdl() == 1 && dataDictionaryPojo.getRds_success() == 0))  {
                exceptionList.add(dataDictionaryPojo);
            }
        }
        return exceptionList;
    }

    public ArrayList<DataDictionaryPojo> getList() {
        return list;
    }

    public Boolean executeUpdate(String xmlFilePath, String dataDictionaryTag, String hiveMateStoreTag) {

        SimpleDateFormat logSdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        final String JDBCConnectionPojo = "com.smallelephant.common.entity.JDBCConnectionPojo";
        JDBCConnectionPojo dataDictionaryConnectionPojo = (JDBCConnectionPojo) XMLReader.getXMInfo(JDBCConnectionPojo, xmlFilePath, dataDictionaryTag, 1);
        JDBCConnectionPojo hive_matestoreConnectionPojo = (JDBCConnectionPojo) XMLReader.getXMInfo(JDBCConnectionPojo, xmlFilePath, hiveMateStoreTag, 1);
        Connection connection;
        System.out.println(logSdf.format(new Date()) + " DataManageMonitor " + "INFO " + DataDictionaryUpdate.class + " XML读取dataDictionaryConnectionPojo信息" + dataDictionaryConnectionPojo.toString());
        System.out.println(logSdf.format(new Date()) + " DataManageMonitor " + "INFO " + DataDictionaryUpdate.class + " XML读取hive_matestoreConnectionPojo信息" + hive_matestoreConnectionPojo.toString());

        /***
         * 查询data_dictionary表，获取DDL更新标识和load2hive标识的表数据
         */
        connection = DataSourceUtil.getConnection(dataDictionaryConnectionPojo);
        if (null == connection) {
            System.out.println(logSdf.format(new Date()) + " DataManageMonitor " + "ERROR " + DataDictionaryUpdate.class + " 查询data_dictionary表，获取JDBC连接失败");
            return false;
        }
        StringBuilder sqlStatementQuery = new StringBuilder();
        sqlStatementQuery.append("select id, business_source, db_source, db_name, table_name, current_ddl, load2hive, storage, table_comment from  data_dictionary where current_ddl =1 and load2hive = 1 and last_update = ? and 1 = 1");
        PreparedStatement pst = null;
        ResultSet resultSet = null;
        try {
            SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
            String date = simpleDateFormat.format(new Date());
            pst = connection.prepareStatement(sqlStatementQuery.toString());
            pst.setString(1, date);
            resultSet = pst.executeQuery();
            DataDictionaryPojo dataDictionary;
            while (resultSet.next()) {
                dataDictionary = new DataDictionaryPojo();
                dataDictionary.setId(resultSet.getInt("id"));
                dataDictionary.setBusinessSource(resultSet.getString("business_source"));
                dataDictionary.setDbSource(resultSet.getString("db_source"));
                dataDictionary.setDbName(resultSet.getString("db_name"));
                dataDictionary.setTableName(resultSet.getString("table_name"));
                dataDictionary.setCurrentDdl(resultSet.getInt("current_ddl"));
                dataDictionary.setLoad2hive(resultSet.getInt("load2hive"));
                dataDictionary.setStorage(resultSet.getInt("storage"));
                dataDictionary.setTableComment(resultSet.getString("table_comment"));
                list.add(dataDictionary);
            }
            System.out.println(logSdf.format(new Date()) + " DataManageMonitor " + "INFO " + DataDictionaryUpdate.class + " 查询data_dictionary表，获取今日发生DDL更新的表数据成功 " + list.size() + "条");
        } catch (SQLException e) {
            System.out.println(logSdf.format(new Date()) + " DataManageMonitor " + "ERROR " + DataDictionaryUpdate.class + " 查询data_dictionary表，获取今日发生DDL更新的表数据失败 " + e.getMessage());
            return false;
        } finally {
            DataSourceUtil.closeAll(resultSet, pst, connection);
        }


        /***
         * 通过查询hive元数据信息存在的Mysql表->验证DDL操作是否生效
         */
        connection = DataSourceUtil.getConnection(hive_matestoreConnectionPojo);
        if (null == connection) {
            System.out.println(logSdf.format(new Date()) + " DataManageMonitor " + "ERROR " + DataDictionaryUpdate.class + " 查询hive元数据信息，获取JDBC连接失败");
            return false;
        }
        StringBuilder sqlStatementQueryForHiveMate = new StringBuilder();
        sqlStatementQueryForHiveMate.append("select count(*) table_exists from DBS AS A, TBLS AS B where A.DB_ID = B.DB_ID ");
        sqlStatementQueryForHiveMate.append("and A.NAME = ? ");
        sqlStatementQueryForHiveMate.append("and B.TBL_NAME = ? ;");
        try {
            pst = connection.prepareStatement(sqlStatementQueryForHiveMate.toString());
            for (DataDictionaryPojo dataDictionaryPojo : list) {
                pst.setString(1, "ods");
                StringBuilder hiveTableName = new StringBuilder();
                hiveTableName.append("ods_").append(dataDictionaryPojo.getBusinessSource()).append("_").append(dataDictionaryPojo.getDbName()).append("_").append(dataDictionaryPojo.getTableName());
                pst.setString(2, hiveTableName.toString().toLowerCase());
                resultSet = pst.executeQuery();
                while (resultSet.next()) {
                    Boolean table_exists = resultSet.getInt("table_exists") == 1;
                    dataDictionaryPojo.setOds_success(table_exists ? 1 : 0);
                }
                resultSet = null;
                System.out.println(logSdf.format(new Date()) + " DataManageMonitor " + "INFO " + DataDictionaryUpdate.class + " 查询hive元数据信息ods查询->更改Current_DDL " + dataDictionaryPojo.toString());
            }
        } catch (SQLException e) {
            e.printStackTrace();
            System.out.println(logSdf.format(new Date()) + " DataManageMonitor " + "ERROR " + DataDictionaryUpdate.class + " 查询hive元数据信息ods查询->验证DDL操作" + sqlStatementQueryForHiveMate + e.getMessage());
            return false;
        } finally {
            DataSourceUtil.closeAll(resultSet, pst, connection);
        }
        /**
         * 判断rds建表是否成功
         */
        connection = DataSourceUtil.getConnection(hive_matestoreConnectionPojo);
        if (null == connection) {
            System.out.println(logSdf.format(new Date()) + " DataManageMonitor " + "ERROR " + DataDictionaryUpdate.class + " 查询hive元数据信息，获取JDBC连接失败");
            return false;
        }
        try {
            pst = connection.prepareStatement(sqlStatementQueryForHiveMate.toString());
            for (DataDictionaryPojo dataDictionaryPojo : list) {
                pst.setString(1, "rds");
                StringBuilder hiveTableName = new StringBuilder();
                hiveTableName.append("rds_").append(dataDictionaryPojo.getBusinessSource()).append("_").append(dataDictionaryPojo.getDbName()).append("_").append(dataDictionaryPojo.getTableName());
                pst.setString(2, hiveTableName.toString().toLowerCase());
                resultSet = pst.executeQuery();
                while (resultSet.next()) {
                    Boolean table_exists = resultSet.getInt("table_exists") == 1;
                    dataDictionaryPojo.setRds_success(table_exists ? 1 : 0);
                }
                resultSet = null;
                System.out.println(logSdf.format(new Date()) + " DataManageMonitor " + "INFO " + DataDictionaryUpdate.class + " 查询hive元数据信息rds信息->更改Current_DDL " + dataDictionaryPojo.toString());
            }
        } catch (SQLException e) {
            e.printStackTrace();
            System.out.println(logSdf.format(new Date()) + " DataManageMonitor " + "ERROR " + DataDictionaryUpdate.class + " 查询hive元数据信息rds信息->验证DDL操作" + sqlStatementQueryForHiveMate + e.getMessage());
            return false;
        } finally {
            DataSourceUtil.closeAll(resultSet, pst, connection);
        }


        /***
         * 更新data_dictionary表，修改currentDDL的值
         */
        /*connection = DataSourceUtil.getConnection(dataDictionaryConnectionPojo);
        if (null == connection) {
            System.out.println(logSdf.format(new Date()) + " DataManageMonitor " + "ERROR " + DataDictionaryUpdate.class + " 更新data_dictionary表，获取JDBC连接失败");
            return false;
        }
        StringBuilder sqlStatementUpdate = new StringBuilder();
        sqlStatementUpdate.append("update data_dictionary set ");
        sqlStatementUpdate.append("current_ddl = ? ");
        sqlStatementUpdate.append("where id = ? ");
        try {
            Long startTime = System.currentTimeMillis();
            pst = connection.prepareStatement(sqlStatementUpdate.toString());
            for (DataDictionaryPojo dataDictionaryPojo : list) {
                *//**
                 * 给Current_DDL赋值
                 *//*
                if ((dataDictionaryPojo.getOds_success() == 1) && (dataDictionaryPojo.getRds_success() == 1)) {
                    dataDictionaryPojo.setCurrentDdl(0);
                } else {
                    dataDictionaryPojo.setCurrentDdl(1);
                }
                pst.setInt(1, dataDictionaryPojo.getCurrentDdl());
                pst.setInt(2, dataDictionaryPojo.getId());
                pst.addBatch();
            }
            int[] rowsCount = pst.executeBatch();
            Long endTime = System.currentTimeMillis();
            System.out.println(logSdf.format(new Date()) + " DataManageMonitor " + "INFO " + DataDictionaryUpdate.class + " 更新data_dictionary表，修改currentDDL操作,批量Update成功 " + rowsCount.length + "条," + "时间：" + (endTime - startTime) + "   " + sqlStatementUpdate);
        } catch (SQLException e) {
            System.out.println(logSdf.format(new Date()) + " DataManageMonitor " + "ERROR " + DataDictionaryUpdate.class + " 更新data_dictionary表，修改currentDDL操作,批量Update错误 " + sqlStatementUpdate + e.getMessage());
            return false;
        } finally {
            DataSourceUtil.closeAll(resultSet, pst, connection);
        }*/

        /**
         * 执行成功返回true
         */
        return true;

    }
}
