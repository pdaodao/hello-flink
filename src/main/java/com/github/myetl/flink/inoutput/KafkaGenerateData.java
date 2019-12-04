package com.github.myetl.flink.inoutput;

import org.apache.flink.table.runtime.util.JsonUtils;
import org.apache.kafka.clients.producer.*;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Random;

/**
 * 生成一些测试数据放入到kafka 的 student 中
 */
public class KafkaGenerateData {

    public static void main(String[] args) throws Exception{

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "127.0.0.1:9092");
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        final String topic = "student";

        Producer<String, String> producer = new KafkaProducer<>(properties);

        final Random random = new Random();
        for (int i = 0; i < 100; i++){
            Thread.sleep(300);

            Map<String, Object> map = new HashMap<>();
            map.put("name", "xiao-"+i);
            map.put("age", 10 + random.nextInt(10));
            map.put("class", "clazz-"+ random.nextInt(5));

            // 发送json 字符串
            producer.send(new ProducerRecord<String, String>(topic,
                    JsonUtils.MAPPER.writeValueAsString(map)));
        }

        producer.flush();
        producer.close();

    }
}
