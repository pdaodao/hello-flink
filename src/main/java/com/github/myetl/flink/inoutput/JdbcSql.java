package com.github.myetl.flink.inoutput;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.java.StreamTableEnvironment;

/**
 * 使用sql的方式读取写入mysql数据库表
 */
public class JdbcSql {
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
        // 定义表结构信息和连接信息
        String input = "CREATE TABLE student (\n" +
                "    name VARCHAR,\n" +
                "    age INT,\n" +
                "    class VARCHAR\n" +
                ") WITH (\n" +
                "    'connector.type' = 'jdbc',\n" +
                "    'connector.url' = 'jdbc:mysql://127.0.0.1:3306/flink',\n" +
                "    'connector.table' = 'student',\n" +
                "    'connector.username' = 'root',\n" +
                "    'connector.password' = 'root'\n" +
                ")";

        String out = "CREATE TABLE stuout (\n" +
                "    name VARCHAR,\n" +
                "    age INT,\n" +
                "    class VARCHAR\n" +
                ") WITH (\n" +
                "    'connector.type' = 'jdbc',\n" +
                "    'connector.url' = 'jdbc:mysql://127.0.0.1:3306/flink',\n" +
                "    'connector.table' = 'stuout',\n" +
                "    'connector.username' = 'root',\n" +
                "    'connector.password' = 'root'\n" +
                ")";

        // sqlUpdate 会把ddl按需求注册为输入输出
        tableEnv.sqlUpdate(input);
        tableEnv.sqlUpdate(out);

        tableEnv.sqlUpdate("insert into stuout select name,age,class from student where age > 16");

        env.execute("jdbc-sql");

    }
}
