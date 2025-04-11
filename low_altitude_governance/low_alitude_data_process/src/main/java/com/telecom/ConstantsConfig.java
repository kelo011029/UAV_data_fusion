package com.telecom;

public class ConstantsConfig {
    // MySQL连接信息
    public static final String MYSQL_HOST = "your_mysql_host";
    public static final int MYSQL_PORT = 3306; // MySQL默认端口是3306
    public static final String MYSQL_DATABASE = "your_database_name";
    public static final String MYSQL_USERNAME = "your_mysql_username";
    public static final String MYSQL_PASSWORD = "your_mysql_password";

    // Kafka信息
    public static final String KAFKA_BOOTSTRAP_SERVERS = "192.168.200.133:9092";
    //topic配置_jm
    public static final String KAFKA_TOPIC_JM = "jm";
    public static final String KAFKA_GROUP_ID_JM = "flink2hive-jm-group";
    public static final String KAFKA_CLIENT_ID_JM = "your_kafka_client_id";

    //topic配置_aoa
    public static final String KAFKA_TOPIC_AOA = "aoa";
    public static final String KAFKA_GROUP_ID_AOA = "flink2hive-aoa-group";
    public static final String KAFKA_CLIENT_ID_AOA = "your_kafka_client_id";

    //topic配置_tdoa
    public static final String KAFKA_TOPIC_TDOA = "tdoa";
    public static final String KAFKA_GROUP_ID_TDOA = "flink2hive-tdoa-group";
    public static final String KAFKA_CLIENT_ID_TDOA = "your_kafka_client_id";

    // Hive连接信息
    public static final String HIVE_METASTORE_URIS = "thrift://192.168.200.133:9083";
    public static final String HIVE_DATABASE = "ods";
    public static final String HIVE_USERNAME = "hive";
    public static final String HIVE_PASSWORD = "your_hive_password";

    // hive catalog 信息
    public static final String CATALOG_HIVECONFDIR = "/opt/hive/conf";
    public static final String CATALOG_DATABASE = "ods";
    public static final String CATALOG_NAME = "hive" ;

    //配置checkpoint路径
    public static final String checkpointPath = "file:///opt/flink_checkpoints/" ;

    // 私有构造函数，防止实例化
    private ConstantsConfig() {
        throw new AssertionError();
    }

}