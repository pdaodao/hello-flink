package com.github.myetl.flink.inoutput;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Json;
import org.apache.flink.table.descriptors.Kafka;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.types.Row;

/**
 *  使用 connect 和 ddl 注册
 */
public class KafkaSql {

    public static void main(String[] args) throws Exception {
        // 运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 使用Blink
        EnvironmentSettings bsSettings = EnvironmentSettings.newInstance()
                .useBlinkPlanner()
                .inStreamingMode()
                .build();

        // table 环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, bsSettings);


        String input = "CREATE TABLE student (\n" +
                "    name VARCHAR,\n" +
                "    age INT,\n" +
                "    class VARCHAR\n" +
                ") WITH (\n" +
                "    'connector.type' = 'kafka',\n" +
                "    'connector.version' = 'universal',\n" +
                "    'connector.topic' = 'student',\n" +
                "    'connector.startup-mode' = 'earliest-offset',\n" +
                "    'connector.properties.0.key' = 'bootstrap.servers',\n" +
                "    'connector.properties.0.value' = '127.0.0.1:9092',\n" +
                "    'connector.properties.1.key' = 'group.id',\n" +
                "    'connector.properties.1.value' = 'kafka/sql',\n" +
                "    'update-mode' = 'append',\n" +
                "    'format.type' = 'json',\n" +
                "    'format.derive-schema' = 'true'\n" +
                ")";

        // DDL 注册
        tableEnv.sqlUpdate(input);

        // 使用 connect 方式 把 stuout 注册为 输入 输出
        tableEnv.connect(new Kafka()
                    .version("universal")
                    .property("bootstrap.servers", "127.0.0.1:9092")
                    .property("group.id", "kafka-sql")
                    .topic("stuout")
                    .startFromEarliest())
                .withFormat(
                    new Json()
                    .deriveSchema())
                .withSchema(new Schema()
                    .field("name", "VARCHAR")
                    .field("age", "INT")
                    .field("class", "VARCHAR"))
                .inAppendMode()
                .registerTableSourceAndSink("stuout");

        // 把 Table 转为 DataStream
        // 转为DataStream 后 可以使用 DataStream Api 进行数据的处理
        // 把数据写入到本地
        tableEnv.toAppendStream(tableEnv.sqlQuery("select name,age ,class from student"), Row.class)
                .writeAsText("h2.txt");


        tableEnv.sqlUpdate("insert into stuout select name,age,class from student where age > 16");

        env.execute("kafka-sql");
    }

}
