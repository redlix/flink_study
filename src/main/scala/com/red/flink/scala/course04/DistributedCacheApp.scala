package com.red.flink.scala.course04

import org.apache.commons.io.FileUtils
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.configuration.Configuration

/**
  * ${desc}
  * <pre>
  * Version         Date            Author          Description
  * ------------------------------------------------------------
  *  1.0.0           2019/06/06     red        -
  * </pre>
  *
  * @author red
  * @version 1.0.0 2019-06-06 11:08
  * @since 1.0.0 
  */
object DistributedCacheApp {
    def main(args: Array[String]): Unit = {
        val env = ExecutionEnvironment.getExecutionEnvironment
        val filePath = "file:///Users/red/tmp/flink/04/hello.txt"

        // step1: 注册一个文件(本地或HDFS)
        env.registerCachedFile(filePath, "scala-dc")

        import org.apache.flink.api.scala._
        val data = env.fromElements("hadoop", "spark", "flink", "pyspark", "storm")

        data.map(new RichMapFunction[String, String] {
            // step2: 在open方法中获取到分布式缓存内容
            override def open(parameters: Configuration): Unit = {
                val dcFile = getRuntimeContext.getDistributedCache().getFile("scala-dc")

                val lines = FileUtils.readLines(dcFile)
                import scala.collection.JavaConverters._
                for (element <- lines.asScala) {
                    println(element)
                }
            }

            override def map(in: String): String = {
                in
            }
        }).print()
    }
}
