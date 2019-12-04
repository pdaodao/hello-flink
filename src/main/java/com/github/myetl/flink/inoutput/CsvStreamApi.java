package com.github.myetl.flink.inoutput;


import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.io.RowCsvInputFormat;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.types.Row;


/**
 * 使用Stream API的方式读取 写入 本地csv文件
 * 输入文件格式为
 *      name,age,class
 *      string,int,string
 */
public class CsvStreamApi {

    public static void main(String[] args) throws Exception{

        String rootDir = CsvStreamApi.class.getResource("/").toURI().getPath();
        // 输入文件路径
        String inFilePath = rootDir+"/student.csv";
        // 输出文件路径 运行后在 target/classes 路径下
        String outFilePath = rootDir+"/out1.csv";

        // 获取运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置并发度为1 可以不设
        env.setParallelism(1);

        // 使用 RowCsvInputFormat 把每一行记录解析为一个 Row
        RowCsvInputFormat csvInput = new RowCsvInputFormat(
                new Path(inFilePath),                                        // 文件路径
                new TypeInformation[]{Types.STRING, Types.INT, Types.STRING},// 字段类型
                "\n",                                             // 行分隔符
                ",");                                            // 字段分隔符
        // 跳过第一行 表头
        csvInput.setSkipFirstLineAsHeader(true);

        // 输入
        DataStreamSource<Row> student =  env.readFile(csvInput, inFilePath);

        // 过滤出年龄大于 16 的行
        DataStream<Row> filtered = student.filter(new FilterFunction<Row>() {
            @Override
            public boolean filter(Row value) throws Exception {
                Object obj = value.getField(1);
                if(obj == null) return false;
                return (int)obj > 16;
            }
        });

        // writeAsCsv 只能处理 Tuple 类型的数据 这里做一下转换，也可以输出为文本
        DataStream<Tuple3<String, Integer, String>> tuple3  = filtered.map(
                new MapFunction<Row, Tuple3<String,Integer, String>>() {
            @Override
            public Tuple3<String, Integer, String> map(Row value) throws Exception {
                return new Tuple3((String)value.getField(0),
                        (Integer)value.getField(1), (String) value.getField(2));
            }
        });

        // 输出到csv文件
        tuple3.writeAsCsv(outFilePath, FileSystem.WriteMode.OVERWRITE);

        // 触发执行
        env.execute();
    }
}
