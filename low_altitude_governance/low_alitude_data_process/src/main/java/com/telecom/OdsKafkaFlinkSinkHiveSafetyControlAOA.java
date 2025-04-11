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



public class OdsKafkaFlinkSinkHiveSafetyControlAOA {
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
                .setTopics(ConstantsConfig.KAFKA_TOPIC_AOA)
                .setGroupId(ConstantsConfig.KAFKA_GROUP_ID_AOA)
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStreamSource<String> kafkaSource = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source");
//        kafkaSource.print("kafkaSource: " + kafkaSource).setParallelism(1);


        //对kafka原始数据分类，新数据和老数据
        //新数据留在主流中 ：processFilter
        //老数据和异常数据在侧输出流中： old_aoa_sideoutput
        OutputTag<String> stringOutputTag = new OutputTag<String>("old_aoa_sideoutput") {};
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
//                    System.out.println("rootNode: " + rootNode);
                    //老的数据没有"devId"，所以使用这个做为新老数据区分标志: devIdStr: 60104
                    JsonNode devIdNode = rootNode.get("devId");
                    JsonNode timestampNode = rootNode.get("timestamp");
//                    System.out.println("devIdNode: " + devIdNode);
//                    System.out.println("timestampNode: " + timestampNode);
                    if (devIdNode != null) {
                        // 如果是新的 JSON 字符串，直接输出
                        if (timestampNode == null) {
                            long timestamp = System.currentTimeMillis();
                            // 如果没有 timestampNode 则添加当前系统时间的时间戳： "timestamp": 1733194523132
                            ObjectNode objectNode = (ObjectNode) rootNode;
                            objectNode.put("timestamp", timestamp);
//                            System.out.println("objectNode.toString(): " + objectNode.toString());
                            out.collect(objectNode.toString());
                        } else
                        {
//                            System.out.println("str: " + str);
                            out.collect(str);
                        }
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
//        SideOutputDataStream<String> oldAOASideoutput = processFilter.getSideOutput(stringOutputTag);
//        oldAOASideoutput.print("oldAOASideoutput: " + oldAOASideoutput).setParallelism(1) ;



        //处理新数据转pojo类用于构建表 ：processFilter
        SingleOutputStreamOperator<AOABean> processData = processFilter.flatMap(new RichFlatMapFunction<String, AOABean>() {
            ObjectMapper objectMapper ;

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                objectMapper = new ObjectMapper();
            }

            @Override
            public void flatMap(String str, Collector<AOABean> out) throws Exception {
                JsonNode rootNode = objectMapper.readTree(str);

                //封装成pojo对象
                AOABean aOABean = new AOABean();
                aOABean.setDevId(rootNode.get("devId").asText());
                aOABean.setBatchno(rootNode.get("batchno").asText());
                aOABean.setDistance(rootNode.get("distance").asDouble());
                aOABean.setLasting_time(rootNode.get("lasting_time").asInt());
                aOABean.setChannel(rootNode.get("channel").asDouble());
                aOABean.setModel(rootNode.get("model").asText());
                aOABean.setDirection(rootNode.get("direction").asDouble());
                aOABean.setTimestamp(rootNode.get("timestamp").asLong());

                out.collect(aOABean);
            }

        });
//        processData.print("processData: " + processData).setParallelism(1);

/**
 *注册表
 *
 * */
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

        //无人机数据: processData,ods_flink_aoa_dh
        Table ods_flink_aoa_dh = tableEnv.fromDataStream(processData);


//        //测试数据
//        TableResult tabledronert = tableEnv.executeSql("select * from " + ods_flink_aoa_dh + " limit 1");
//        tabledronert.print();
//        String sqlstr = "select * from " + ods_flink_aoa_dh + " limit 1" ;
//        TableResult tabrut1 = tableEnv.executeSql(sqlstr);
//        tabrut1.print();



/** 处理jm 无人机数据 : ods_flink_jm_drone_dh
 *1.创建jm 无人机数据hive表 :ods_kafkaflink_jm_drone_dh
 *2.把流式数据表数据写入hive表
 * */

// 创建 hive jm 表
        String create_kafkaflink_aoa_tb =
                "CREATE TABLE IF NOT EXISTS ods.ods_kafkaflink_aoa_dh (" +
                        "devId      String ," +
                        "batchno    String ," +
                        "distance   double ," +
                        "lasting_time  bigint ," +
                        "channel    double ," +
                        "model      String ," +
                        "direction  double ," +
                        "`timestamp`  bigint ," +
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
//        System.out.println("create_kafkaflink_aoa_tb: " + create_kafkaflink_aoa_tb);
        TableResult resut1 = tableEnv.executeSql(create_kafkaflink_aoa_tb);

        // Kafka jm 主题表 流式写入 hive jm 表
        String insert_kafkaflink_aoa_tb =
                "INSERT INTO ods.ods_kafkaflink_aoa_dh " +
                        "SELECT " +
                        "devId     ," +
                        "batchno   ," +
                        "distance  ," +
                        "lasting_time ," +
                        "channel   ," +
                        "model     ," +
                        "direction ," +
                        "`timestamp` ," +
                        "from_unixtime(`timestamp` / 1000, 'yyyy-MM-dd') as dt," +
                        "from_unixtime(`timestamp` / 1000, 'HH') as hr " +
                        "FROM " + ods_flink_aoa_dh ;
//        System.out.println("insert_kafkaflink_aoa_tb: " + insert_kafkaflink_aoa_tb);
        TableResult resut2 = tableEnv.executeSql(insert_kafkaflink_aoa_tb);
//        //测试数据
//        resut2.print();


/**
 * 以上是数据开发过程，下面是提交流式开发环境
 *
 * */

        //提交执行流环境
        env.execute("classname: OdsKafkaFlinkSinkHiveSafetyControlAOA");

    }
}
