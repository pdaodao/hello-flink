package com.github.myetl.flink.inoutput;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.io.jdbc.JDBCInputFormat;
import org.apache.flink.api.java.io.jdbc.JDBCOutputFormat;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.types.Row;

/**
 *  使用 DataStream API 方式
 *  从mysql数据库中读取数据经过简单的处理后写入到mysql中
 */
public class JdbcStreamApi {

    public static void main(String[] args) throws Exception {
        // 获取运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置并发度为1 可以不设
        env.setParallelism(1);

        // 输入  数据库连接和数据表信息
        JDBCInputFormat inputFormat = JDBCInputFormat
                .buildJDBCInputFormat()
                .setDrivername("com.mysql.jdbc.Driver")
                .setDBUrl("jdbc:mysql://127.0.0.1:3306/flink?useUnicode=true&characterEncoding=UTF8&serverTimezone=GMT%2B8")
                .setUsername("root")
                .setPassword("root")
                .setQuery("select name,age,class from student")
                .setRowTypeInfo(
                        new RowTypeInfo(new TypeInformation[]{Types.STRING, Types.INT, Types.STRING},
                                new String[]{"name", "age", "class"}))
                .finish();

        // 输出 数据库连接和数据表信息
        JDBCOutputFormat outputFormat = JDBCOutputFormat
                .buildJDBCOutputFormat()
                .setDrivername("com.mysql.jdbc.Driver")
                .setDBUrl("jdbc:mysql://127.0.0.1:3306/flink?useUnicode=true&characterEncoding=UTF8&serverTimezone=GMT%2B8")
                .setUsername("root")
                .setPassword("root")
                .setQuery("insert into stuout(name,age,class) values (?,?,?)")
                .finish();

        // 输入
        DataStreamSource<Row> input = env.createInput(inputFormat);

        // 过滤出年龄大于16的记录
        DataStream<Row> filtered =  input.filter(new FilterFunction<Row>() {
            @Override
            public boolean filter(Row value) throws Exception {
                return (int) value.getField(1) > 16;
            }
        });

        // 输出
        filtered.writeUsingOutputFormat(outputFormat);

        env.execute("JdbcStreamApi");
    }
}
