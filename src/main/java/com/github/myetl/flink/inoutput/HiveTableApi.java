package com.github.myetl.flink.inoutput;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.catalog.hive.HiveCatalog;


/**
 * hive中数据表的读写
 */
public class HiveTableApi {

    public static void main(String[] args) throws Exception{
        // 运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 使用Blink 当前版本的flink hive 只支持在batch模式下写
        EnvironmentSettings bsSettings = EnvironmentSettings.newInstance()
                .useBlinkPlanner()
                .inBatchMode()
                .build();

        // table 环境
        TableEnvironment tableEnv = TableEnvironment.create(bsSettings);

        // hive-site.xml 所在目录地址
        String rootDir = CsvStreamApi.class.getResource("/").toURI().getPath();

        String name            = "default";
        String defaultDatabase = "default";
        String hiveConfDir     = rootDir;
        String version         = "1.2.1"; // 2.3.4 or 1.2.1


        HiveCatalog hive = new HiveCatalog(name, defaultDatabase, hiveConfDir, version);
        tableEnv.registerCatalog(name, hive);
        tableEnv.useCatalog(name);

        tableEnv.sqlQuery("select * from student where age > 16")
                 .insertInto("stuout");

//        tableEnv.sqlUpdate("insert into stuout select * from student where age > 16");

        // 输出到本地文件
//        Table src = tableEnv.sqlQuery("select * from student where age > 16");
//        tableEnv.toAppendStream(src, Row.class)
//                .writeAsText("hive.txt");

        tableEnv.execute("hive");
    }
}
