package com.red.flink.scala.course05

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment

/**
  * ${desc}
  * <pre>
  * Version         Date            Author          Description
  * ------------------------------------------------------------
  *  1.0.0           2019/06/10     red        -
  * </pre>
  *
  * @author red
  * @version 1.0.0 2019-06-10 08:09
  * @since 1.0.0 
  */
object DataStreamSourceAPP {
    def main(args: Array[String]): Unit = {
        val env = StreamExecutionEnvironment.getExecutionEnvironment

        //        socketFunction(env)
        //        nonParallelSourceFunction(env)
        //        parallelSourceFunction(env)
        richParallelSourceFunction(env)
        env.execute("DataStreamSourceAPP")
    }


    def richParallelSourceFunction(streamExecutionEnvironment: StreamExecutionEnvironment): Unit = {
        val data = streamExecutionEnvironment.addSource(new CustomRichParallelSourceFunction).setParallelism(2)
        data.print()
    }

    def parallelSourceFunction(executionEnvironment: StreamExecutionEnvironment): Unit = {
        val data = executionEnvironment.addSource(new CustomParallelSourceFunction).setParallelism(2)
        data.print().setParallelism(2)
    }

    def nonParallelSourceFunction(executionEnvironment: StreamExecutionEnvironment): Unit = {
        val data = executionEnvironment.addSource(new CustomNonParallelSourceFunction).setParallelism(1)
        data.print()
    }

    def socketFunction(executionEnvironment: StreamExecutionEnvironment): Unit = {
        val data = executionEnvironment.socketTextStream("localhost", 9999)
        data.print().setParallelism(1)
    }
}
