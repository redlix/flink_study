package com.red.flink.scala.course04

import org.apache.flink.api.common.accumulators.LongCounter
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.core.fs.FileSystem.WriteMode

/**
  * ${desc}
  * <pre>
  * Version         Date            Author          Description
  * ------------------------------------------------------------
  *  1.0.0           2019/06/06     red        -
  * </pre>
  *
  * @author red
  * @version 1.0.0 2019-06-06 10:33
  * @since 1.0.0 
  */
object CounterApp {
    def main(args: Array[String]): Unit = {
        val env = ExecutionEnvironment.getExecutionEnvironment

        val data = env.fromElements("hadoop", "spark", "flink", "pyspark", "storm")

        //        data.map(new RichMapFunction[String, Long]() {
        //            var counter = 0l
        //
        //            override def map(in: String): Long = {
        //                counter = counter + 1
        //                println("counter: " + counter)
        //                counter
        //            }
        //        }).setParallelism(4).print()

        val info = data.map(new RichMapFunction[String, String]() {
            // step1: 定义计数器
            val counter = new LongCounter()

            override def open(parameters: Configuration): Unit = {
                // step2: 注册计数器
                getRuntimeContext.addAccumulator("element-counts-scala", counter)
            }

            override def map(in: String): String = {
                counter.add(1)
                in
            }
        })

        val filePath = "file:///Users/red/tmp/flink/04/scala_counter_sink_out"
        info.writeAsText(filePath, WriteMode.OVERWRITE).setParallelism(3)
        val jobObject = env.execute("element-counts-scala")

        // step3: 获取计数器
        val num = jobObject.getAccumulatorResult[Long]("element-counts-scala")

        print("num: " + num)
    }
}
