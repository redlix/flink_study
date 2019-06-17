package com.red.flink.scala.course07

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time

/**
  * ${desc}
  * <pre>
  * Version         Date            Author          Description
  * ------------------------------------------------------------
  *  1.0.0           2019/06/17     red        -
  * </pre>
  *
  * @author red
  * @version 1.0.0 2019-06-17 09:03
  * @since 1.0.0 
  */
object WindowsApp {
    def main(args: Array[String]): Unit = {
        val env = StreamExecutionEnvironment.getExecutionEnvironment

        import org.apache.flink.api.scala._

        val text = env.socketTextStream("localhost", 9999)

        text.flatMap(_.split(","))
            .map((_, 1)).keyBy(0)
            // sliding time windows.
            //            .timeWindow(Time.seconds(10), Time.seconds(5))
            //tumbling time windows.
            .timeWindow(Time.seconds(5))
            .sum(1)
            .print()
            .setParallelism(1)

        env.execute("WindowsApp")
    }
}
