package com.github.myetl.flink.inoutput;

import com.google.common.collect.Lists;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.http.HttpHost;


/**
 * 读elasticsearch 写到本地
 */
public class ElasticsearchStreamApi {

    public static void main(String[] args) throws Exception{
        // 获取运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置并发度为1 可以不设
        env.setParallelism(1);

        RowTypeInfo rowTypeInfo = new RowTypeInfo(
                new TypeInformation[]{Types.STRING, Types.INT, Types.STRING},
                new String[]{"name", "age", "class"});

        ElasticsearchInput es =  ElasticsearchInput.builder(
                Lists.newArrayList(new HttpHost("127.0.0.1", 9200)),
                "student")
                .setRowTypeInfo(rowTypeInfo)
                .build();

        // 输入
        DataStreamSource source = env.createInput(es);

        // 把结果输出到本地文件
        source.writeAsText("es-student.txt", FileSystem.WriteMode.OVERWRITE);

        // 触发运行
        env.execute("esStreamApi");
    }

}
