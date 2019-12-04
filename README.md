# Hello-Flink  

 
基于Apache Flink 1.9 使用BlinkPlanner学习Flink的使用。

## Flink是什么 
Apache Flink 是一个分布式流批一体化的开源平台。Flink 的核心是一个提供数据分发、通信以及自动容错的流计算引擎。Flink 在流计算之上构建批处理，并且原生的支持迭代计算，内存管理以及程序优化。

## 项目构建

- 构建maven项目
    ```
    mvn archetype:generate                               \
          -DarchetypeGroupId=org.apache.flink              \
          -DarchetypeArtifactId=flink-quickstart-java      \
          -DarchetypeVersion=1.9.0
    ```
    它将以交互式的方式询问你项目的 groupId、artifactId 和 package 名称。
    
    pom.xml 中有些Flink的依赖作用范围是provided，在本地运行时需要选中add-dependencies-for-IDEA这个profile，集群环境打包时取消选中该profile,因为集群环境中已经存在该些jar。

- pom.xml 依赖信息

   基础功能依赖上面构建中的依赖即可。
   
   使用 Table Api & Sql 时 需要加入下面的依赖。
   ```
   <dependency>
        <groupId>org.apache.flink</groupId>
        <artifactId>flink-table-planner-blink_2.11</artifactId>
        <version>${flink.version}</version>
    </dependency>
   ```
   
   从不同的输入输出源中读写数据需要加入相应的依赖。
   

- 编写Flink程序的基本步骤如下：
    1. 获取运行环境(execution environment)
    2. 加载/创建初始数据
    3. 对数据进行转换处理
    4. 指定处理后数据的输出位置
    5. 触发程序的运行
---

## 基础入门-输入输出篇

在基础入门篇中先不介绍Flink的集群环境，直接在Ide中运行Flink程序代码(程序在启动时会启动一个多线程本地环境)，对于集群环境来说代码不需要做任何修改只需提交到集群环境中运行即可。

在该输入输出篇中分别以Datastream API 和 Table API & SQL的方式介绍几种常用的输入输出:
 
-  [输入输出-csv](docs/1.Flink输入输出-csv.md)  
-  [输入输出-jdbc](docs/2.Flink输入输出-jdbc.md)
-  [输入输出-kafka](docs/3.Flink输入输出-kafka.md)
-  [输入输出-hive](docs/4.Flink输入输出-hive.md)
-  [输入输出-elasticsearch](docs/5.Flink输入输出-elasticsearch.md)



## 基础入门-数据处理篇 todo 
数据处理的基本流程是对输入数据经过一系列的处理后落地到输出中。输入输出篇中主要介绍了数据的读取和写入相关知识。在数据处理篇中主要讲解在Flink中如何处理分布式数据。
