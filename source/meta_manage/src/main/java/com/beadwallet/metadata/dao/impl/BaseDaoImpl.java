package com.beadwallet.metadata.dao.impl;

import com.beadwallet.common.utils.datasourceutil.DataSourceUtil;
import com.beadwallet.metadata.dao.BaseDao;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Repository;

import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @ClassName BaseDaoImpl
 * @Description
 * @Author kai wu
 * @Date 2019/1/18 11:30
 * @Version 1.0
 */
@Repository
public class BaseDaoImpl implements BaseDao {

    Logger logger = LoggerFactory.getLogger(BaseDaoImpl.class);

    /**
     * @param connection
     * @param sql
     * @return boolean
     * @Author kai wu
     * @Description execute sql
     * @Date 2019/1/18 11:30
     * @Param [connection, sql]
     */
    @Override
    public boolean execute(Connection connection, String sql) {
        boolean result = false;
        PreparedStatement pst = null;
        try {
            pst = connection.prepareStatement(sql);
            result = pst.execute();
            logger.info("execute: {} successfully !", sql);
        } catch (SQLException e) {
            e.printStackTrace();
            logger.error("execute: {} failed,SQLException：{}!", sql, e.getMessage());
        } finally {
            DataSourceUtil.closeStatement(pst);
        }
        return result;
    }


    /**
     * @param connection
     * @param sql
     * @return ResultSet
     * @Author kai wu
     * @Description execute query sql
     * @Date 2019/1/18 11:30
     * @Param [connection, sql]
     */
    @Override
    public List<Map<String,Object>> executeQuery(Connection connection, String sql){
        PreparedStatement pst = null;
        ResultSet rs = null;
        List<Map<String,Object>> list = new ArrayList();
        try {
            pst = connection.prepareStatement(sql);
            rs = pst.executeQuery();
            ResultSetMetaData md = rs.getMetaData();
            int columnCount = md.getColumnCount();
            while (rs.next()) {
                Map<String,Object> rowData = new HashMap();
                for (int i = 1; i <= columnCount; i++) {
                    rowData.put(md.getColumnName(i), rs.getObject(i));
                }
                list.add(rowData);
            }
            logger.info("executeQuery: {} successfully !", sql);
        } catch (SQLException e) {
            e.printStackTrace();
            logger.error("execute: {} failed,SQLException：{}!", sql, e.getMessage());
        }finally {
            DataSourceUtil.closeStatement(pst);
            DataSourceUtil.closeResultSet(rs);
        }
        return  list;
    }
}
