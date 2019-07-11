package com.beadwallet.cons;

public class Constants {
    public static final String JDBC_CLASS_NAME = "com.beadwallet.utils.jdbc.JDBCConnectionPojo";
    public static final String JDBC_TAG_AZKABAN = "AzkabanJdbc";
    public static final String JDBC_TAG_METASTORE = "Metastore";

    public static final String AZ_DISPATCH_BEAN_CLASS_NAME = "com.beadwallet.bean.ConfigBean";
    public static final String AZ_DISPATCH_TAG_NAME = "Dispatch";

    public static final String FLOW_FILE_PREFIX = "etl";

    public static final String FLOW_FILE_EXPANDED_NAME = "flow.job";
    public static final String FLOW_FILE_PREFIX_CREATE = "create_";
    public static final String FLOW_FILE_PREFIX_ODS = "etl_ods_";
    public static final String EXPANDED_NAME_ZIP = ".zip";

    public static final String FLOW_OPE_TYPE_CREATE = "create";
    public static final String FLOW_OPE_TYPE_DELETE = "delete";
    public static final String FLOW_OPE_TYPE_UPDATE = "update";
}