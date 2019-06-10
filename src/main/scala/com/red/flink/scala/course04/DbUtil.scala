package com.red.flink.scala.course04

import scala.util.Random

/**
  * ${desc}
  * <pre>
  * Version         Date            Author          Description
  * ------------------------------------------------------------
  *  1.0.0           2019/06/04     red        -
  * </pre>
  *
  * @author red
  * @version 1.0.0 2019-06-04 16:06
  * @since 1.0.0 
  */
object DbUtil {
    def getConnection() = {
        new Random().nextInt(10) + ""
    }

    def returnConnection(connection: String): Unit = {

    }
}
