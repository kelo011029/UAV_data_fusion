package com.telecom;

import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SideOutputDataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.table.api.*;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.hive.HiveCatalog;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.table.shaded.com.ibm.icu.text.SimpleDateFormat;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Date;


public class OdsKafkaFlinkSinkHiveSafetyControlJM {
    public static void main(String[] args) throws Exception {

        //获取文件名称
        String className = GetClassNameUtil.getCurrentClassNameWithoutDots();
        System.out.println("当前类的路径名称: " + className);

        //配置checkpoint路径
        String checkpointPath = ConstantsConfig.checkpointPath + className;
        System.out.println("checkpointPath: " + checkpointPath);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();


        // 启用 Checkpoint，设置 Checkpoint 间隔为 3 秒
        env.enableCheckpointing(3000L, CheckpointingMode.EXACTLY_ONCE);
        // 设置 Checkpoint 存储目录
//        env.getCheckpointConfig().setCheckpointStorage("file:///opt/flink_checkpoints");
        env.getCheckpointConfig().setCheckpointStorage(checkpointPath);
//        // 设置状态后端为 RocksDB，并指定状态存储目录
//        env.setStateBackend(new EmbeddedRocksDBStateBackend("hdfs://namenode:9000/flink/checkpoints"));
//        env.setStateBackend(new FsStateBackend(checkpointPath));

        //重启策略配置
        //固定延时重启策略：固定重启3次，每隔xx重启一次
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3,Time.minutes(5)));

        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers(ConstantsConfig.KAFKA_BOOTSTRAP_SERVERS)
                .setTopics(ConstantsConfig.KAFKA_TOPIC_JM)
                .setGroupId(ConstantsConfig.KAFKA_GROUP_ID_JM)
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStreamSource<String> kafkaSource = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source");
//        kafkaSource.print("kafkaSource: " + kafkaSource).setParallelism(1);


        //对kafka原始数据分类，新数据和老数据
        //新数据留在主流中 ：processFilter
        //老数据和异常数据在侧输出流中： old_jm_sideoutput
        OutputTag<String> stringOutputTag = new OutputTag<String>("old_jm_sideoutput") {};
        SingleOutputStreamOperator<String> processFilter = kafkaSource.process(new ProcessFunction<String, String>() {
            ObjectMapper objectMapper ;
            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                objectMapper = new ObjectMapper();
            }

            @Override
            public void processElement(String str, ProcessFunction<String, String>.Context ctx, Collector<String> out) throws Exception {
                JsonNode rootNode;
                try {
                    rootNode = objectMapper.readTree(str);
                    rootNode = objectMapper.readTree(rootNode.asText());
                    JsonNode droneNode = rootNode.path("drone");
                    boolean isNewDroneJson = droneNode.isArray();
                    if (isNewDroneJson) {
                        // 如果是新的 JSON 字符串，直接输出
                        out.collect(str);
                    } else {
                        // 如果不是新的 JSON 字符串，输出到侧输出流
                        ctx.output(stringOutputTag,str);
                    }
                } catch (Exception e) {
                    // 处理异常，输出到侧输出流
                    ctx.output(stringOutputTag, str);
//                    e.printStackTrace();
                }
            }
        });
//        processFilter.print("processFilter: " + processFilter).setParallelism(1) ;
//        //side data
//        SideOutputDataStream<String> oldJmSideoutput = processFilter.getSideOutput(stringOutputTag);
//        oldJmSideoutput.print("oldJmSideoutput: " + oldJmSideoutput).setParallelism(1) ;

        //处理新数据转pojo类用于构建表 ：processFilter
        //对新数据中的drone json数组拆解为多行
        //使用flatmap对一条数据流转成多条数据流
        SingleOutputStreamOperator<JMBean> processData = processFilter.flatMap(new RichFlatMapFunction<String, JMBean>() {
            ObjectMapper objectMapper ;

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                objectMapper = new ObjectMapper();
            }

            @Override
            public void flatMap(String str, Collector<JMBean> out) throws Exception {
                JsonNode rootNodeStr = objectMapper.readTree(str);
                JsonNode rootNode = objectMapper.readTree(rootNodeStr.asText());
                JsonNode droneNode = rootNode.get("drone");
                String station_id = rootNode.get("station_id").asText();

                //遍历json数组拼接成多行
                for (JsonNode drone : droneNode) {
//                System.out.println("打印 droneNode:" + drone);
                    String dronestring = drone.toString();
                    // 解析输入的 JSON 字符串为 JsonNode 对象
                    JsonNode droneStrNode = objectMapper.readTree(dronestring);
                    // 添加 "station_id" 字段
                    ((ObjectNode) droneStrNode).put("station_id", station_id);
//                    System.out.println("打印 droneStrNode:" + droneStrNode );

                    //封装成pojo对象
                    JMBean jmBean = new JMBean();
                    jmBean.setBatch_num(droneStrNode.get("batch_num").asText());
                    jmBean.setDetect_type(droneStrNode.get("detect_type").asInt());
                    jmBean.setDrone_uuid(droneStrNode.get("drone_uuid").asText());
                    jmBean.setFrequency(droneStrNode.get("frequency").asDouble());
                    jmBean.setHeight(droneStrNode.get("height").asDouble());
                    jmBean.setIntrusion_start_time(droneStrNode.get("intrusion_start_time").asText());
                    jmBean.setLasting_time(droneStrNode.get("lasting_time").asInt());
                    jmBean.setLatitude(droneStrNode.get("latitude").asDouble());
                    jmBean.setLongitude(droneStrNode.get("longitude").asDouble());
                    jmBean.setModel(droneStrNode.get("model").asText());
                    // scanID是嵌套的json数组，这里不做解析直接转字符串输出
                    // 数据格式： "scanID":[{"id":"92025"}]
                    jmBean.setScanID(droneStrNode.get("scanID").toString());
                    jmBean.setSpeed(droneStrNode.get("speed").asDouble());
                    jmBean.setType(droneStrNode.get("type").asInt());
                    jmBean.setStation_id(droneStrNode.get("station_id").asText());

                    //处理时间戳: timestamp
                    String batchNum = droneStrNode.get("batch_num").asText().split("ㆍ")[1];
                    SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmssSSS");
                    try {
                        Date date = sdf.parse(batchNum);
                        long timestamp = date.getTime() + droneStrNode.get("lasting_time").asInt();
//                        System.out.println("时间: " + batchNum + ",时间戳: " + timestamp);
                        jmBean.setTimestamp(timestamp);

                        // 转换为 LTZ TIMESTAMP
                        ZonedDateTime ts_ltz = Instant.ofEpochMilli(timestamp).atZone(ZoneId.systemDefault());
//                        // 带时区的时间: 2024-11-27T06:42:05.452+08:00[GMT+08:00]
//                        System.out.println("带时区的时间，中间有T: " + ts_ltz);

                        // 转换为 LTZ TIMESTAMP 字符串
                        LocalDateTime ltzTimestamp = Instant.ofEpochMilli(timestamp)
                                .atZone(ZoneId.systemDefault()) // 默认时区，或指定 ZoneId.of("Asia/Shanghai")
                                .toLocalDateTime();
                        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");
                        // 格式化输出-字符串
                        String ts_ltz_str = ltzTimestamp.format(formatter);
//                        System.out.println("Formatted LTZ Timestamp: " + ts_ltz_str);
                        // 格式化输出-Timestamp
                        Timestamp ltz_timestamp = Timestamp.valueOf(ltzTimestamp);
//                        System.out.println("ltz_timestamp: " + ltz_timestamp);
                        jmBean.setTs_ltz(ltz_timestamp);

                    } catch (Exception e) {
                        //给默认时间 2024-01-01 00:00:00/1704038400000
                        Long ts = 1704038400000L ;
                        jmBean.setTimestamp(1704038400000L);
                        jmBean.setTs_ltz(Timestamp.valueOf("2024-01-01 00:00:00"));
                        e.printStackTrace();
                    }

                    out.collect(jmBean);
                }
            }
        });
//        processData.print("processData: " + processData).setParallelism(1);


        //对主流数据区分 类型（0-遥控器1-无人机）
        //主流是无人机：droneData
        //测输出流是遥控器： remoteData
        OutputTag<JMBean> remoteOutputTag = new OutputTag<JMBean>("remoteData_sideoutput") {};
        SingleOutputStreamOperator<JMBean> droneData = processData.process(new ProcessFunction<JMBean, JMBean>() {
            @Override
            public void processElement(JMBean jmBean, ProcessFunction<JMBean, JMBean>.Context ctx, Collector<JMBean> out) throws Exception {
                try {
                    int type = jmBean.getType();
                    if (type == 1) {
                        // 如果是新的 JSON 字符串，直接输出
                        out.collect(jmBean);
                    } else {
                        // 如果不是新的 JSON 字符串，输出到侧输出流
                        ctx.output(remoteOutputTag, jmBean);
                    }
                } catch (Exception e) {
                    // 处理异常，输出到侧输出流
                    ctx.output(remoteOutputTag, jmBean);
//                    e.printStackTrace();
                }
            }
        });
        //主流 无人机数据
//        droneData.print("droneData: " + droneData).setParallelism(1);
        //测输出流 遥控器数据
        SideOutputDataStream<JMBean> remoteData = droneData.getSideOutput(remoteOutputTag);
//        remoteData.print("remoteData: " + remoteData).setParallelism(1);



//        //处理老的jm数据 转成pojo类 ：oldJmSideoutput
//        //转成 JMBeanOld 类数据： jmBeanOldData
//        //对字符串判断如果不是json格式就给默认值
//        SingleOutputStreamOperator<JMBeanOld> jmBeanOldData = oldJmSideoutput.map(new RichMapFunction<String, JMBeanOld>() {
//            JsonNode rootNode;
//            ObjectMapper objectMapper ;
//
//            @Override
//            public void open(Configuration parameters) throws Exception {
//                super.open(parameters);
//                objectMapper = new ObjectMapper();
//            }
//
//            @Override
//            public JMBeanOld map(String str) throws Exception {
//                JMBeanOld jmBeanOld = new JMBeanOld();
//
//                try {
//                    rootNode = objectMapper.readTree(str);
////                    System.out.println("rootNode: " + rootNode);
////                    System.out.println("intrusion_start_time: " + rootNode.get("intrusion_start_time"));
//
//                    //封装成pojo对象
//                    jmBeanOld.setIntrusion_start_time(rootNode.get("intrusion_start_time").asText());
//                    jmBeanOld.setLatitude(rootNode.get("latitude").asDouble());
//                    jmBeanOld.setTarget_type(rootNode.get("target_type").asInt());
//                    jmBeanOld.setDroneUK(rootNode.get("droneUK").asText());
//                    jmBeanOld.setBatch_num(rootNode.get("batch_num").asText());
//                    jmBeanOld.setSpeed(rootNode.get("speed").asDouble());
//                    jmBeanOld.setFrequency(rootNode.get("frequency").asDouble());
//                    jmBeanOld.setDriver_longitude(rootNode.get("driver_longitude").asDouble());
//                    jmBeanOld.setLasting_time(rootNode.get("lasting_time").asInt());
//                    jmBeanOld.setDataType(rootNode.get("data_type").asInt());
//                    jmBeanOld.setModel(rootNode.get("model").asText());
//                    jmBeanOld.setDriver_latitude(rootNode.get("driver_latitude").asDouble());
//                    jmBeanOld.setLongitude(rootNode.get("longitude").asDouble());
//                    jmBeanOld.setHeight(rootNode.get("height").asDouble());
//                    jmBeanOld.setTimestamp(rootNode.get("timestamp").asLong());
//
//                } catch (Exception e) {
//                    // 处理异常，给null
////                    jmBeanOld = null ;
//                    jmBeanOld.setBatch_num(str);
////                    e.printStackTrace();
//                }
//                return jmBeanOld;
//
//            }
//        });
////        jmBeanOldData.print("jmBeanOldData: " + jmBeanOldData).setParallelism(1);




        //配置hive表执行环境
        EnvironmentSettings settings = EnvironmentSettings.inStreamingMode();
//        TableEnvironment tableEnv = TableEnvironment.create(settings);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //配置hive catalog 元数据链接
        String name = ConstantsConfig.CATALOG_NAME;
        String defaultDatabase = ConstantsConfig.CATALOG_DATABASE;
        String hiveConfDir = ConstantsConfig.CATALOG_HIVECONFDIR;
        HiveCatalog hive = new HiveCatalog(name, defaultDatabase, hiveConfDir);
        tableEnv.registerCatalog("flink2hive", hive);
        // set the HiveCatalog as the current catalog of the session
        tableEnv.useCatalog("flink2hive");

        //方言选择：控制sql的语法 -暂时不需要 -使用默认
//        tableEnv.getConfig().setSqlDialect(SqlDialect.DEFAULT);
//        tableEnv.getConfig().setSqlDialect(SqlDialect.HIVE);

//        //测的是hive是否链接成功
//        String sqlstr="show databases" ;
//        TableResult tableResult = tableEnv.executeSql(sqlstr);
//        tableResult.print();


/**
 *注册表
 *
 * */
        //无人机数据: droneData,ods_flink_jm_drone_dh
        Table ods_flink_jm_drone_dh = tableEnv.fromDataStream(droneData);
        //遥控器数据: remoteData,ods_flink_jm_remote_dh
        Table ods_flink_jm_remote_dh = tableEnv.fromDataStream(remoteData);
        //JMBeanOld 类数据： jmBeanOldData,ods_flink_jm_old_dh
//        Table ods_flink_jm_old_dh = tableEnv.fromDataStream(jmBeanOldData);

        //测试数据
//        TableResult tabledronert = tableEnv.executeSql("select intrusion_start_time from " + ods_flink_jm_drone_dh + " limit 1");
//        TableResult tabledronert = tableEnv.executeSql("select * from " + ods_flink_jm_drone_dh + " limit 1");
//        tabledronert.print();
//        String sqlstr = "select * from " + ods_flink_jm_drone_dh + " limit 1" ;
//        TableResult tabrut1 = tableEnv.executeSql(sqlstr);
//        tabrut1.print();


//
///** 处理jm 老数据 : ods_flink_jm_old_dh
// *1.创建jm 老数据hive表 :ods_kafkaflink_jm_old_dh
// *2.把流式数据表数据写入hive表
// * */
//
//// 创建 hive jm 表
//        String create_kafkaflink_jm_old_tb =
//                "CREATE TABLE IF NOT EXISTS ods.ods_kafkaflink_jm_old_dh (" +
//                        "intrusion_start_time   String  ," +
//                        "latitude               double  ," +
//                        "target_type            int     ," +
//                        "droneuk                String  ," +
//                        "batch_num              String  ," +
//                        "speed                  double  ," +
//                        "frequency              double  ," +
//                        "driver_longitude       double  ," +
//                        "lasting_time           int     ," +
//                        "data_type              int     ," +
//                        "model                  String  ," +
//                        "driver_latitude        double  ," +
//                        "longitude              double  ," +
//                        "height                 double  ," +
//                        "`timestamp`            bigint  ," +
//                        "dt STRING  ," +
//                        "hr STRING  " +
//                        ") PARTITIONED BY (dt, hr)  WITH (" +
//                        "'connector' = 'hive'," +
//                        "  'partition.time-extractor.timestamp-pattern'='$dt $hr:00:00'," +
//                        "  'sink.partition-commit.trigger'='process-time'," +
//                        "  'sink.partition-commit.delay'='1 h'," +
//                        "  'sink.partition-commit.watermark-time-zone'='Asia/Shanghai', " +
//                        "  'sink.partition-commit.policy.kind'='metastore,success-file'" +
//                        ")";
////        System.out.println("create_kafkaflink_jm_old_tb: " + create_kafkaflink_jm_old_tb);
//        TableResult ods_kafkaflink_jm_dh = tableEnv.executeSql(create_kafkaflink_jm_old_tb);
//
//        // Kafka jm 主题表 流式写入 hive jm 表
//        String insert_kafkaflink_jm_old_tb =
//                "INSERT INTO ods_kafkaflink_jm_old_dh " +
//                    "SELECT " +
//                        "intrusion_start_time," +
//                        "latitude," +
//                        "target_type," +
//                        "droneUK," +
//                        "batch_num," +
//                        "speed," +
//                        "frequency," +
//                        "driver_longitude," +
//                        "lasting_time," +
//                        "data_type," +
//                        "model," +
//                        "driver_latitude," +
//                        "longitude," +
//                        "height," +
//                        "`timestamp`," +
//                        "from_unixtime(`timestamp` / 1000, 'yyyy-MM-dd') as dt," +
//                        "from_unixtime(`timestamp` / 1000, 'HH') as hr " +
//                        "FROM " + ods_flink_jm_old_dh ;
////        System.out.println("insert_kafkaflink_jm_old_tb: " + insert_kafkaflink_jm_old_tb);
//        TableResult kafkajm2HiveResult = tableEnv.executeSql(insert_kafkaflink_jm_old_tb);
//
////        //测试数据
////        TableResult tbret1 = tableEnv.executeSql(insert_kafkaflink_jm_old_tb);
////        tbret1.print();




/** 处理jm 无人机数据 : ods_flink_jm_drone_dh
 *1.创建jm 无人机数据hive表 :ods_kafkaflink_jm_drone_dh
 *2.把流式数据表数据写入hive表
 * */

// 创建 hive jm 表
        String create_kafkaflink_jm_drone_tb =
                "CREATE TABLE IF NOT EXISTS ods.ods_kafkaflink_jm_drone_dh (" +
                        "batch_num            String ," +
                        "detect_type          int    ," +
                        "drone_uuid           String ," +
                        "frequency            double ," +
                        "height               double ," +
                        "intrusion_start_time String ," +
                        "lasting_time         int    ," +
                        "latitude             double ," +
                        "longitude            double ," +
                        "model                String ," +
                        "scanid               String ," +
                        "speed                double ," +
                        "type                 int    ," +
                        "station_id           String ," +
                        "`timestamp`          bigint  ," +
                        "dt STRING  ," +
                        "hr STRING  " +
                        ") PARTITIONED BY (dt, hr)  WITH (" +
                        "'connector' = 'hive'," +
                        "  'partition.time-extractor.timestamp-pattern'='$dt $hr:00:00'," +
                        "  'sink.partition-commit.trigger'='process-time'," +
                        "  'sink.partition-commit.delay'='1 h'," +
                        "  'sink.partition-commit.watermark-time-zone'='Asia/Shanghai', " +
                        "  'sink.partition-commit.policy.kind'='metastore,success-file'" +
                        ")";
//        System.out.println("create_kafkaflink_jm_drone_tb: " + create_kafkaflink_jm_drone_tb);
        TableResult resut1 = tableEnv.executeSql(create_kafkaflink_jm_drone_tb);

        // Kafka jm 主题表 流式写入 hive jm 表
        String insert_kafkaflink_jm_drone_tb =
                "INSERT INTO ods_kafkaflink_jm_drone_dh " +
                        "SELECT " +
                        "batch_num           ," +
                        "detect_type         ," +
                        "drone_uuid          ," +
                        "frequency           ," +
                        "height              ," +
                        "intrusion_start_time," +
                        "lasting_time        ," +
                        "latitude            ," +
                        "longitude           ," +
                        "model               ," +
                        "scanID              ," +
                        "speed               ," +
                        "type                ," +
                        "station_id          ," +
                        "`timestamp`         ," +
                        "from_unixtime(`timestamp` / 1000, 'yyyy-MM-dd') as dt," +
                        "from_unixtime(`timestamp` / 1000, 'HH') as hr " +
                        "FROM " + ods_flink_jm_drone_dh ;
//        System.out.println("insert_kafkaflink_jm_drone_tb: " + insert_kafkaflink_jm_drone_tb);
        TableResult resut2 = tableEnv.executeSql(insert_kafkaflink_jm_drone_tb);
//        //测试数据
//        resut2.print();





/** 处理jm 遥控器数据 : ods_flink_jm_remote_dh
 *1.创建jm 遥控器数据hive表 :ods_kafkaflink_jm_remote_dh
 *2.把流式数据表数据写入hive表
 * */

// 创建 hive jm 表
        String create_kafkaflink_jm_remote_tb =
                "CREATE TABLE IF NOT EXISTS ods.ods_kafkaflink_jm_remote_dh (" +
                        "batch_num            String ," +
                        "detect_type          int    ," +
                        "drone_uuid           String ," +
                        "frequency            double ," +
                        "height               double ," +
                        "intrusion_start_time String ," +
                        "lasting_time         int    ," +
                        "latitude             double ," +
                        "longitude            double ," +
                        "model                String ," +
                        "scanid               String ," +
                        "speed                double ," +
                        "type                 int    ," +
                        "station_id           String ," +
                        "`timestamp`          bigint  ," +
                        "dt STRING  ," +
                        "hr STRING  " +
                        ") PARTITIONED BY (dt, hr)  WITH (" +
                        "'connector' = 'hive'," +
                        "  'partition.time-extractor.timestamp-pattern'='$dt $hr:00:00'," +
                        "  'sink.partition-commit.trigger'='process-time'," +
                        "  'sink.partition-commit.delay'='1 h'," +
                        "  'sink.partition-commit.watermark-time-zone'='Asia/Shanghai', " +
                        "  'sink.partition-commit.policy.kind'='metastore,success-file'" +
                        ")";
//        System.out.println("create_kafkaflink_jm_remote_tb: " + create_kafkaflink_jm_remote_tb);
        TableResult resut3 = tableEnv.executeSql(create_kafkaflink_jm_remote_tb);

        // Kafka jm 主题表 流式写入 hive jm 表
        String insert_kafkaflink_jm_remote_tb =
                "INSERT INTO ods_kafkaflink_jm_remote_dh " +
                "SELECT " +
                        "batch_num           ," +
                        "detect_type         ," +
                        "drone_uuid          ," +
                        "frequency           ," +
                        "height              ," +
                        "intrusion_start_time," +
                        "lasting_time        ," +
                        "latitude            ," +
                        "longitude           ," +
                        "model               ," +
                        "scanID              ," +
                        "speed               ," +
                        "type                ," +
                        "station_id          ," +
                        "`timestamp`         ," +
                        "from_unixtime(`timestamp` / 1000, 'yyyy-MM-dd') as dt," +
                        "from_unixtime(`timestamp` / 1000, 'HH') as hr " +
                        "FROM " + ods_flink_jm_remote_dh ;
//        System.out.println("insert_kafkaflink_jm_remote_tb: " + insert_kafkaflink_jm_remote_tb);
        TableResult resut4 = tableEnv.executeSql(insert_kafkaflink_jm_remote_tb);
//        //测试数据
//        resut4.print();





/**
 * 以上是数据开发过程，下面是提交流式开发环境
 *
 * */

        //提交执行流环境
        env.execute("classname: OdsKafkaFlinkSinkHiveSafetyControlJM");

    }
}
