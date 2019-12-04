package com.github.myetl.flink.inoutput;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.sinks.CsvTableSink;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.table.sources.CsvTableSource;

/**
 * 使用TableAPI的方式读取 写入 本地csv文件
 */
public class CsvTableApi {

    public static void main(String[] args) throws Exception {

        String rootDir = CsvStreamApi.class.getResource("/").toURI().getPath();
        // 输入文件路径
        String inFilePath = rootDir + "/student.csv";
        // 输出文件路径 运行后在 target/classes 路径下
        String outFilePath = rootDir + "/out.csv";

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


        // csv 输入源
        CsvTableSource csvTableSource = CsvTableSource.builder()
                .path(inFilePath)
                .fieldDelimiter(",")
                .ignoreFirstLine()
                .field("name", Types.STRING)
                .field("age", Types.INT)
                .field("class", Types.STRING)
                .build();

        // 注册为数据表
        tableEnv.registerTableSource("student", csvTableSource);

        // csv 输出
        TableSink csvTableSink = new CsvTableSink(outFilePath, ",", 1, FileSystem.WriteMode.OVERWRITE)
                .configure(new String[]{"name", "age", "class"},
                new TypeInformation[]{Types.STRING, Types.INT, Types.STRING});

        // 注册为数据表
        tableEnv.registerTableSink("stuout", csvTableSink);

        // 执行sql查询 查询后的数据输出到out中
        tableEnv.sqlQuery("select name,age,class from student where age > 16").insertInto("stuout");
        // 或者 下面的方法 把查询后的数据输出到out
//        tableEnv.sqlUpdate("insert into stuout (select name,age,class from student where age > 16)");

        env.execute();
    }
}
