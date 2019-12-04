package com.github.myetl.flink.inoutput;


import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.formats.json.JsonRowDeserializationSchema;
import org.apache.flink.formats.json.JsonRowSerializationSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.flink.types.Row;
import org.apache.kafka.clients.producer.ProducerRecord;
import javax.annotation.Nullable;
import java.util.Properties;

/**
 * 使用 Stream Api方式从 kafka 中读写 数据
 */
public class KafkaStreamApi {

    public static void main(String[] args) throws Exception{
        // 获取运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置并发度为1 可以不设
        env.setParallelism(1);

        // kafka 连接信息
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "127.0.0.1:9092");
        properties.setProperty("group.id", "KafkaStreamApi");

        // 反序列化 把byte[]转为 Row
        JsonRowDeserializationSchema schema = new JsonRowDeserializationSchema.Builder(new RowTypeInfo(
                new TypeInformation[]{Types.STRING, Types.INT, Types.STRING},
                new String[]{"name", "age", "class"}
        ))
                .failOnMissingField()
                .build();

        // 创建 kafka 消费者
        FlinkKafkaConsumer<Row> input = new FlinkKafkaConsumer<Row>("student", schema , properties);
        input.setStartFromEarliest();

        // 序列化 把 Row 转为 byte[]
        JsonRowSerializationSchema outSchema = new JsonRowSerializationSchema
                .Builder(schema.getProducedType()).build();

        // 创建 kafka 生产者
        FlinkKafkaProducer<Row> output = new FlinkKafkaProducer<Row>("stuout",
                new KafkaSerializationSchema<Row>() {
                    @Override
                    public ProducerRecord<byte[], byte[]> serialize(Row element, @Nullable Long timestamp) {
                        return new ProducerRecord<byte[], byte[]>("stuout", outSchema.serialize(element));
                    }
                },
                properties, FlinkKafkaProducer.Semantic.AT_LEAST_ONCE);


        // 1. 读取 kafka 的 student
        DataStreamSource<Row> studentSource =  env.addSource(input);

        // 2. 过滤出年龄大于 16 的记录
        DataStream<Row> filtered = studentSource.filter(new FilterFunction<Row>() {
            @Override
            public boolean filter(Row value) throws Exception {

                return (int) value.getField(1) > 16;
            }
        });

        // 把结果输出到本地文件
        filtered.writeAsText("kafka-student.txt", FileSystem.WriteMode.OVERWRITE);

        // 3. 输出到 kafka 的 stuout 中
        filtered.addSink(output);

        // 触发运行
        env.execute("KafkaStreamApi");
    }
}
