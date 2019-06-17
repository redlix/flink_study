package com.red.flink.scala.course08

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer

/**
  * ${desc}
  * <pre>
  * Version         Date            Author          Description
  * ------------------------------------------------------------
  *  1.0.0           2019/06/17     red        -
  * </pre>
  *
  * @author red
  * @version 1.0.0 2019-06-17 13:54
  * @since 1.0.0
  *
  * 生产数据
  * ./kafka-console-producer.sh -broker-list master:9092 --topic test
  * 查看topic
  * ./kafka-topics.sh --list -zookeeper master:2181
  */
object KafkaConnectorConsumerApp {
    def main(args: Array[String]): Unit = {
        val env = StreamExecutionEnvironment.getExecutionEnvironment

        // checkpoint常用设置参数
        env.enableCheckpointing(4000)
        env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
        env.getCheckpointConfig.setCheckpointTimeout(10000)
        env.getCheckpointConfig.setMaxConcurrentCheckpoints(1)


        import org.apache.flink.api.scala._

        val topic = "test"
        val properties = new Properties()

        // hadoop000   必须要求你的idea这台机器的hostname和ip的映射关系必须要配置
        properties.setProperty("bootstrap.servers", "master:9092")
        properties.setProperty("group.id", "test")

        val data = env.addSource(new FlinkKafkaConsumer[String](topic, new SimpleStringSchema(), properties))

        data.print()

        env.execute("KafkaConnectorConsumerApp")
    }
}
