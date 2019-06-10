package com.red.flink.scala.course05

import java.{lang, util}

import org.apache.flink.streaming.api.collector.selector.OutputSelector
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

/**
  * ${desc}
  * <pre>
  * Version         Date            Author          Description
  * ------------------------------------------------------------
  *  1.0.0           2019/06/10     red        -
  * </pre>
  *
  * @author red
  * @version 1.0.0 2019-06-10 08:27
  * @since 1.0.0 
  */
object DataStreamTransformationApp {
    def main(args: Array[String]): Unit = {
        val env = StreamExecutionEnvironment.getExecutionEnvironment

        //        filterFunction(env)
        //        unionFunction(env)
        splitSelectFunction(env)
        env.execute("DataStreamTransformationApp")
    }


    def splitSelectFunction(streamExecutionEnvironment: StreamExecutionEnvironment): Unit = {
        import org.apache.flink.api.scala._
        val data = streamExecutionEnvironment.addSource(new CustomParallelSourceFunction)
        val splits = data.split(new OutputSelector[Long] {
            override def select(out: Long): lang.Iterable[String] = {
                val list = new util.ArrayList[String]()
                if (out % 2 == 0) {
                    list.add("even")
                } else {
                    list.add("odd")
                }
                list
            }
        })
        splits.select("even").print().setParallelism(1)
    }

    def unionFunction(streamExecutionEnvironment: StreamExecutionEnvironment): Unit = {
        import org.apache.flink.api.scala._
        val data1 = streamExecutionEnvironment.addSource(new CustomParallelSourceFunction)
        val data2 = streamExecutionEnvironment.addSource(new CustomParallelSourceFunction)

        data1.union(data2).print().setParallelism(1)
    }

    def filterFunction(streamExecutionEnvironment: StreamExecutionEnvironment): Unit = {
        import org.apache.flink.api.scala._
        val data = streamExecutionEnvironment.addSource(new CustomParallelSourceFunction).setParallelism(2)

        data.map(x => {
            println("receive: " + x)
            x
        }).filter(_ % 2 == 0).print().setParallelism(2)
    }
}
