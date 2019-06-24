package com.imooc.flink.scala.course02

import org.apache.flink.api.common.operators.Order
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.core.fs.FileSystem.WriteMode

/**
  * 使用Scala开发Flink的批处理应用程序
  */
object BatchWCScalaApp {


  def main(args: Array[String]): Unit = {

    val input = "file:///Users/red/Desktop/temp/news/data/sj_data/all_data/all_seg_word_data.txt"

    val env = ExecutionEnvironment.getExecutionEnvironment

    val text = env.readTextFile(input)

    // 引入隐式转换
    import org.apache.flink.api.scala._

    // TODO... 1) 参考Scala课程  2）API再来讲
    val data = text.flatMap(_.toLowerCase.split(" "))
      .filter(_.nonEmpty)
      .map((_,1))
      .groupBy(0)
      .sum(1)


    data.sortPartition(1, Order.DESCENDING).writeAsText("file:///Users/red/Desktop/temp/news/data/sj_data/all_data/count.txt", WriteMode.OVERWRITE).setParallelism(1)

    env.execute("BatchWCScalaApp")
  }
}
