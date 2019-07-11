package com.smallelephant.datamodul.logmonitor;

import com.smallelephant.common.entity.DataRecordsUpdatePojo;
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


public class DataRecordsUpdate {

    private ArrayList<DataRecordsUpdatePojo> list = new ArrayList<DataRecordsUpdatePojo>();

    public ArrayList<DataRecordsUpdatePojo> getList() {
        return list;
    }

    public ArrayList<DataRecordsUpdatePojo> getExceptionList() {
        ArrayList<DataRecordsUpdatePojo> exceptionList = new ArrayList<>();
        for (DataRecordsUpdatePojo dataRecordsUpdatePojo : list) {
            if (!dataRecordsUpdatePojo.isUpdate_status()) {
                exceptionList.add(dataRecordsUpdatePojo);
            }
        }
        return exceptionList;
    }


    /***
     * 该方法将data_update_records表补全，实现了将业务表日常增量和入库数据的核对，将今天的入库状态发送邮件通知工作人员；
     * @param xmlFilePath xml配置文件的地址
     * @param dataUpdateRecordsTag dataUpdateRecords表在xml中的配置项标签
     * @param hiveJDBCTag hiveJDBC在xml中的配置项标签
     * @return
     */
    public Boolean executeUpdate(String xmlFilePath, String dataUpdateRecordsTag, String hiveJDBCTag) {

        SimpleDateFormat logSdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        final String JDBCConnectionPojo = "com.smallelephant.common.entity.JDBCConnectionPojo";
        JDBCConnectionPojo mysqlConnectionPojo = (JDBCConnectionPojo) XMLReader.getXMInfo(JDBCConnectionPojo, xmlFilePath, dataUpdateRecordsTag, 1);
        JDBCConnectionPojo hiveConnectionPojo = (JDBCConnectionPojo) XMLReader.getXMInfo(JDBCConnectionPojo, xmlFilePath, hiveJDBCTag, 1);
        hiveConnectionPojo.setUrl(hiveConnectionPojo.getUrl() + "/ods");

        Connection connection;
        PreparedStatement pst = null;
        ResultSet resultSet = null;

        System.out.println(logSdf.format(new Date()) + " DataManageMonitor " + "INFO " + DataRecordsUpdate.class + " XML读取data_update_recordsConnectionPojo信息,Tag=" + dataUpdateRecordsTag + mysqlConnectionPojo.toString());
        System.out.println(logSdf.format(new Date()) + " DataManageMonitor " + "INFO " + DataRecordsUpdate.class + " XML读取hive-JDBC信息,Tag=" + hiveJDBCTag + hiveConnectionPojo.toString());

        /***
         * 查询data_update_records表，获取今天发生更新的所有表的数据，存到list中
         */
        connection = DataSourceUtil.getConnection(mysqlConnectionPojo);
        if (null == connection) {
            System.out.println(logSdf.format(new Date()) + " DataManageMonitor " + "ERROR " + DataRecordsUpdate.class + " 查询data_update_records表,获取JDBC连接失败");
            return false;
        } else {
            SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
            String date = simpleDateFormat.format(new Date());
            StringBuilder sqlStatementQuery = new StringBuilder();
            sqlStatementQuery.append("select distinct ");
            sqlStatementQuery.append("A.id,");
            sqlStatementQuery.append("B.business_source,");
            sqlStatementQuery.append("A.db_name,");
            sqlStatementQuery.append("A.table_name,");
            sqlStatementQuery.append("increase_num,export_num,");
            sqlStatementQuery.append("update_status,update_date ");
            sqlStatementQuery.append("from ");
            sqlStatementQuery.append("data_update_records A,");
            sqlStatementQuery.append("data_dictionary B ");
            sqlStatementQuery.append("where A.id=B.id ");
            sqlStatementQuery.append("and update_date = ? ");
            sqlStatementQuery.append("and current_ddl = 0 ");
            sqlStatementQuery.append("and 1=1");
            try {
                pst = connection.prepareStatement(sqlStatementQuery.toString());
                pst.setString(1, date);
                resultSet = pst.executeQuery();
                while (resultSet.next()) {
                    DataRecordsUpdatePojo recordsUpdatePojo = new DataRecordsUpdatePojo();
                    recordsUpdatePojo.setId(resultSet.getInt("id"));
                    recordsUpdatePojo.setBusiness_source(resultSet.getString("business_source"));
                    recordsUpdatePojo.setDb_name(resultSet.getString("db_name"));
                    recordsUpdatePojo.setTable_name(resultSet.getString("table_name"));
                    recordsUpdatePojo.setIncrease_num(resultSet.getLong("increase_num"));
                    recordsUpdatePojo.setExport_num(resultSet.getLong("export_num"));
                    recordsUpdatePojo.setUpdate_status(resultSet.getBoolean("update_status"));
                    recordsUpdatePojo.setUpdate_date(resultSet.getDate("update_date"));
                    list.add(recordsUpdatePojo);
                }
                System.out.println(logSdf.format(new Date()) + " DataManageMonitor " + "INFO " + DataRecordsUpdate.class + " 查询data_update_records表,执行SQL语句成功 " + list.size() + "条");
            } catch (SQLException e) {
                System.out.println(logSdf.format(new Date()) + " DataManageMonitor " + "ERROR " + DataRecordsUpdate.class + " 查询data_update_records表,执行SQL语句失败" + sqlStatementQuery.toString() + e.getMessage());
                return false;
            } finally {
                DataSourceUtil.closeAll(resultSet, pst, connection);
            }
        }


        /***
         * -----hive_JDBC----
         * 从list中拿取表数据，去hive中查询分区表的数据量,补全空缺字段
         */
        connection = DataSourceUtil.getConnection(hiveConnectionPojo);
        if (null == connection) {
            System.out.println(logSdf.format(new Date()) + " DataManageMonitor " + "ERROR " + DataRecordsUpdate.class + " 查询hive数仓ods层,获取HIVE-JDBC连接失败");
            return false;
        } else {
            SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
            int dt = Integer.parseInt(sdf.format(new Date()));
            try {
                StringBuilder hqlStatementQuery;
                for (DataRecordsUpdatePojo dataRecordsUpdatePojo : list) {
                    hqlStatementQuery = new StringBuilder();
                    hqlStatementQuery.append("select count(*) as export_num from ").append("ods.");
                    hqlStatementQuery.append("ods_").append(dataRecordsUpdatePojo.getBusiness_source()).append("_").append(dataRecordsUpdatePojo.getDb_name()).append("_").append(dataRecordsUpdatePojo.getTable_name()).append(" ");
                    hqlStatementQuery.append("where dt = ? ");

                    pst = connection.prepareStatement(hqlStatementQuery.toString());
                    pst.setInt(1, dt);
                    resultSet = pst.executeQuery();
                    while (resultSet.next()) {
                        dataRecordsUpdatePojo.setExport_num(resultSet.getInt("export_num"));
                    }
                    //实际入库数量要微大于等于records表中的数量
                    if (dataRecordsUpdatePojo.getIncrease_num() <= dataRecordsUpdatePojo.getExport_num()) {
                        dataRecordsUpdatePojo.setUpdate_status(true);
                    } else {
                        dataRecordsUpdatePojo.setUpdate_status(false);
                    }
                    resultSet = null;
                    System.out.println(logSdf.format(new Date()) + " DataManageMonitor " + "INFO " + DataRecordsUpdate.class + " 查询hive数仓ods层,补全入库数量  " + hqlStatementQuery.toString() + dt);
                }
            } catch (SQLException e) {
                System.out.println(logSdf.format(new Date()) + " DataManageMonitor " + "ERROR " + DataRecordsUpdate.class + " 查询hive数仓ods层,执行sql语句失败" + e.getMessage());
                return false;
            } finally {
                DataSourceUtil.closeAll(resultSet, pst, connection);
            }
        }


        /***
         * 将数据更新到data_update_records表
         */
        connection = DataSourceUtil.getConnection(mysqlConnectionPojo);
        if (null == connection) {
            System.out.println(new Date() + " DataManageMonitor " + "ERROR " + "更新ata_update_records表,获取JDBC连接失败");
            return false;
        } else {
            StringBuilder sqlStatementUpdate = new StringBuilder();
            sqlStatementUpdate.append("update data_update_records set ");
            sqlStatementUpdate.append("export_num = ? ,");
            sqlStatementUpdate.append("update_status = ? ");
            sqlStatementUpdate.append("where id = ? ");
            sqlStatementUpdate.append("and update_date = ? ");
            sqlStatementUpdate.append("and 1=1");
            try {
                int[] rowsCount = {};
                Long startTime = System.currentTimeMillis();
                pst = connection.prepareStatement(sqlStatementUpdate.toString());
                for (DataRecordsUpdatePojo dataRecordsUpdatePojo : list) {
                    pst.setLong(1, dataRecordsUpdatePojo.getExport_num());
                    pst.setBoolean(2, dataRecordsUpdatePojo.isUpdate_status());
                    pst.setInt(3, dataRecordsUpdatePojo.getId());
                    pst.setDate(4, dataRecordsUpdatePojo.getUpdate_date());
                    pst.addBatch();
                }
                rowsCount = pst.executeBatch();
                Long endTime = System.currentTimeMillis();
                System.out.println(new Date() + " DataManageMonitor " + "INFO " + "更新data_update_records表,批量Update成功 " + rowsCount.length + "条," + "时间：" + (endTime - startTime) + "   " + sqlStatementUpdate);
            } catch (SQLException e) {
                e.printStackTrace();
                System.out.println(new Date() + " DataManageMonitor " + "ERROR " + "更新data_dictionary表，修改currentDDL操作,批量Update错误 " + sqlStatementUpdate + e.getMessage());
                return false;
            } finally {
                DataSourceUtil.closeAll(resultSet, pst, connection);
            }
        }

        return true;
    }

}
