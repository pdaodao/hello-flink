package com.github.myetl.flink.inoutput;


import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.io.jdbc.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.java.StreamTableEnvironment;

/**
 * 使用TableApi的方式读取 写入mysql中的数据表
 *
 */
public class JdbcTableApi {

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

        // 输入表相关数据库连接信息
        JDBCOptions jdbcOptions = JDBCOptions.builder()
                .setDriverName("com.mysql.jdbc.Driver")
                .setDBUrl("jdbc:mysql://127.0.0.1:3306/flink")
                .setUsername("root")
                .setPassword("root")
                .setTableName("student")
                .build();

        // 表结构
        TableSchema studentSchema = TableSchema.builder()
                .field("name", DataTypes.STRING())
                .field("age", DataTypes.INT())
                .field("class", DataTypes.STRING())
                .build();

        // 1 注册输入表
        tableEnv.registerTableSource("student", JDBCTableSource
                .builder()
                .setOptions(jdbcOptions)
                .setSchema(studentSchema)
                .build() );

        // 2 注册输出
        tableEnv.registerTableSink("stuout", JDBCAppendTableSink
                .builder()
                .setDrivername("com.mysql.jdbc.Driver")
                .setDBUrl("jdbc:mysql://127.0.0.1:3306/flink")
                .setUsername("root")
                .setPassword("root")
                .setQuery("insert into stuout(name,age,class) values (?,?,?)")
                .setParameterTypes(Types.STRING, Types.INT, Types.STRING)
                .build()
                .configure(
                        new String[]{"name", "age", "class"},
                        new TypeInformation[]{Types.STRING, Types.INT, Types.STRING}
                )
        );

        // Table Api 方式 处理 数据 等同与下面的sql
        tableEnv
                .scan("student").select("name,age,class")
                .filter("age > 16")
                .insertInto("stuout");

        // sql 方式处理数据
//        tableEnv.sqlUpdate("insert into stuout " +
//                "select name,age,class from student where age > 16");

        env.execute("JdbcTableApi");
    }

}
