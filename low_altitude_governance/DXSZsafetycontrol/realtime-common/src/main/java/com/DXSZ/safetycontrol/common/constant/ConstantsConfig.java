package com.DXSZ.safetycontrol.common.constant;

public class ConstantsConfig {
    // MySQL连接信息
    public static final String MYSQL_HOST = "your_mysql_host";
    public static final int MYSQL_PORT = 3306; // MySQL默认端口是3306
    public static final String MYSQL_DATABASE = "your_database_name";
    public static final String MYSQL_USERNAME = "your_mysql_username";
    public static final String MYSQL_PASSWORD = "your_mysql_password";

    // Kafka信息
    public static final String KAFKA_BOOTSTRAP_SERVERS = "192.168.200.133:9092";
    public static final String KAFKA_TOPIC = "tdoa";
    public static final String KAFKA_GROUP_ID = "your_kafka_group_id";
    public static final String KAFKA_CLIENT_ID = "your_kafka_client_id";

    // Hive信息
    public static final String HIVE_METASTORE_URIS = "thrift://192.168.200.133:9083";
    public static final String HIVE_DATABASE = "your_hive_database_name";
    public static final String HIVE_USERNAME = "your_hive_username";
    public static final String HIVE_PASSWORD = "your_hive_password";

    // 私有构造函数，防止实例化
    private ConstantsConfig() {
        throw new AssertionError();
    }

}
