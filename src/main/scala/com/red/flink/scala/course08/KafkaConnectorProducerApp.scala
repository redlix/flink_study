package com.red.flink.scala.course08

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer
import org.apache.flink.streaming.connectors.kafka.internals.KeyedSerializationSchemaWrapper

/**
  * ${desc}
  * <pre>
  * Version         Date            Author          Description
  * ------------------------------------------------------------
  *  1.0.0           2019/06/17     red        -
  * </pre>
  *
  * @author red
  * @version 1.0.0 2019-06-17 14:17
  * @since 1.0.0
  *        kafka消费者命令
  *        ./kafka-console-consumer.sh --bootstrap-server master:9092 --topic test
  */
object KafkaConnectorProducerApp {
    def main(args: Array[String]): Unit = {
        val env = StreamExecutionEnvironment.getExecutionEnvironment

        // checkpoint常用设置参数
        //        env.enableCheckpointing(4000)
        //        env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
        //        env.getCheckpointConfig.setCheckpointTimeout(10000)
        //        env.getCheckpointConfig.setMaxConcurrentCheckpoints(1)

        val data = env.socketTextStream("localhost", 9999)

        val topic = "test"
        val properties = new Properties()

        properties.setProperty("bootstrap.servers", "master:9092")

        val kafkaSink = new FlinkKafkaProducer[String](topic,
            new SimpleStringSchema(),
            properties)

        data.addSink(kafkaSink)

        env.execute("KafkaConnectorProducerApp")
    }
}
