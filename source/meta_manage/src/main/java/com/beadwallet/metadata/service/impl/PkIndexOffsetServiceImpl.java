package com.beadwallet.metadata.service.impl;

import com.beadwallet.common.entity.IndexOffsetRecord;
import com.beadwallet.common.entity.JdbcConnectionEntity;
import com.beadwallet.common.utils.datasourceutil.DataSourceUtil;
import com.beadwallet.common.utils.xmlutil.XMLReader;
import com.beadwallet.metadata.dao.IndexOffsetRecordDao;
import com.beadwallet.metadata.service.PkIndexOffsetService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;

/**
 * @ClassName PkIndexOffsetServiceImpl
 * @Description
 * @Author kai wu
 * @Date 2019/3/21 15:17
 * @Version 1.0
 */
@Service
public class PkIndexOffsetServiceImpl implements PkIndexOffsetService {

    Logger logger = LoggerFactory.getLogger(PkIndexOffsetServiceImpl.class);

    @Autowired
    private IndexOffsetRecordDao indexOffsetRecordDao;

    @Value("${jdbc.connection.entity}")
    private String jdbcEntity;

    @Value("${metastore.type}")
    private String metaStoreType;

    @Value("${hive.type}")
    private String hiveType;



    /**
     * 该方法用于查询hive数仓中按主键进行etl业务表的最大主键值
     *
     * @return boolean
     * @Date 2019/3/21 18:16
     * @Param []
     **/
    @Override
    public boolean pkIndexOffsetCount(String xmlPath) {
        boolean result = false;

        try {
            //1.解析xml文件，获取数据字典元数据库MySQL配置信息
            ArrayList<JdbcConnectionEntity> metaList = XMLReader.getXMInfo(jdbcEntity, xmlPath, metaStoreType);
            JdbcConnectionEntity meta = metaList.get(0);
            Connection metaConnection = DataSourceUtil.getConnection(meta);

            ArrayList<JdbcConnectionEntity> hiveList = XMLReader.getXMInfo(jdbcEntity, xmlPath, hiveType);
            JdbcConnectionEntity hive = hiveList.get(0);
            Connection hiveConnection = DataSourceUtil.getConnection(hive);

            //2.在元数据库查询所需业务表，并组装成所需sql
            StringBuffer metaSql = new StringBuffer();
            metaSql.append("select a.id,b.business_source,b.db_name,b.table_name,c.column_name  ");
            metaSql.append("from (select id,max(offset) offset,insert_time from index_offset_records where insert_time = (current_date-1) group by id) a  ");
            metaSql.append("inner join data_dictionary b on a.id = b.id ");
            metaSql.append("inner join data_schema_detail c on a.id = c.id ");
            metaSql.append("where b.use_specified_index = 1 and c.column_key = 1 ");
            List<IndexOffsetRecord> indexOffsetRecordList = indexOffsetRecordDao.queryList(metaConnection,metaSql.toString());
            List<String> sqlList = assemblySql(indexOffsetRecordList);

            //3.查询hive数仓中按主键进行etl业务表的最大主键值
            for(String sql:sqlList){
                indexOffsetRecordDao.queryFromHiveAndInsertToMeta(hiveConnection,metaConnection,sql);
            }

        }catch (Exception e){
            e.printStackTrace();
        }

        return result;
    }

    /**
     * 组装查询sql
     *
     * @return List<String>
     * @Date 2019/3/21 18:16
     * @Param []
     **/
    public List<String> assemblySql(List<IndexOffsetRecord> list){
        List<String> resultList = new ArrayList();
        for (IndexOffsetRecord indexOffsetRecord : list) {
                int id = indexOffsetRecord.getId();
                String businessSource = indexOffsetRecord.getBusinessSource();
                String dbName = indexOffsetRecord.getDbName();
                String table = indexOffsetRecord.getTableName();
                String columnKey = indexOffsetRecord.getColumnKey();
                StringBuffer sb = new StringBuffer();
                sb.append("select ");
                sb.append(" " + id + " as id,");
                sb.append(" max("+columnKey+") as `offset` , ");
                sb.append("current_date() as insert_time ");
                sb.append("from ods.ods_" + businessSource + "_"+dbName+"_"+ table + " ");
            resultList.add(sb.toString());
        }
        return  resultList;
    }

}
