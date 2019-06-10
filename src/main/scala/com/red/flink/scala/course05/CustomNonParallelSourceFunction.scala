package com.red.flink.scala.course05

import org.apache.flink.streaming.api.functions.source.SourceFunction

/**
  * ${desc}
  * <pre>
  * Version         Date            Author          Description
  * ------------------------------------------------------------
  *  1.0.0           2019/06/10     red        -
  * </pre>
  *
  * @author red
  * @version 1.0.0 2019-06-10 08:06
  * @since 1.0.0 
  */
class CustomNonParallelSourceFunction extends SourceFunction[Long] {
    var count = 1L

    var isRunning = true

    override def run(sourceContext: SourceFunction.SourceContext[Long]): Unit = {
        while (isRunning) {
            sourceContext.collect(count)
            count += 1
            Thread.sleep(1000)
        }
    }

    override def cancel(): Unit = {
        isRunning = false
    }
}
