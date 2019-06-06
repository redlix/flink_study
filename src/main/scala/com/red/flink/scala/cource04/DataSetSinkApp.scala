package com.red.flink.scala.cource04

import org.apache.flink.api.scala.ExecutionEnvironment
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
  * @version 1.0.0 2019-06-06 10:25
  * @since 1.0.0 
  */
object DataSetSinkApp {
    def main(args: Array[String]): Unit = {
        val env = ExecutionEnvironment.getExecutionEnvironment

        import org.apache.flink.api.scala._
        val data = 1.to(10)
        val text = env.fromCollection(data)

        val filePath = "/Users/red/tmp/flink/04/scala_sink_out"

        text.writeAsText(filePath, WriteMode.OVERWRITE).setParallelism(5)

        env.execute("DataSetSinkApp")
    }
}
