package com.red.flink.scala.course04

import com.red.flink.java.course04.model.Person
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.configuration.Configuration

/**
  * ${desc}
  * <pre>
  * Version         Date            Author          Description
  * ------------------------------------------------------------
  *  1.0.0           2019/06/04     red        -
  * </pre>
  *
  * @author red
  * @version 1.0.0 2019-06-04 09:28
  * @since 1.0.0 
  */
object DataSetDataSource {
    def main(args: Array[String]): Unit = {
        val env = ExecutionEnvironment.getExecutionEnvironment
        fromCollection(env)
        fromTextFile(env)
        fromDir(env)
        fromCsvFile(env)
        readRecursiveFiles(env)
        readCompressionFiles(env)
    }

    def fromCollection(env: ExecutionEnvironment): Unit = {
        import org.apache.flink.api.scala._
        val data = 1 to 10
        env.fromCollection(data).print()
    }

    def fromTextFile(env: ExecutionEnvironment): Unit = {
        val filePath = "file:///Users/red/tmp/flink/04/hello.txt"
        env.readTextFile(filePath).print()
    }

    def fromDir(executionEnvironment: ExecutionEnvironment): Unit = {
        val filePath = "file:///Users/red/tmp/flink/04/inputs"
        executionEnvironment.readTextFile(filePath).print()
    }

    def fromCsvFile(executionEnvironment: ExecutionEnvironment): Unit = {
        import org.apache.flink.api.scala._

        val filePath = "file:///Users/red/tmp/flink/04/people.csv"
        //        executionEnvironment.readCsvFile[(String, Int, String)](filePath, ignoreFirstLine = true).print()
        //        executionEnvironment.readCsvFile[(String, Int)](filePath, ignoreFirstLine = true, includedFields = Array(0, 1)).print()
        //
        //        case class MyCaseClass(name: String, age: Int)
        //        executionEnvironment.readCsvFile[MyCaseClass](filePath, ignoreFirstLine = true, includedFields = Array(0, 1)).print()

        executionEnvironment.readCsvFile[Person](filePath, ignoreFirstLine = true, pojoFields = Array("name", "age", "work")).print()
    }

    def readRecursiveFiles(executionEnvironment: ExecutionEnvironment): Unit = {
        val filePath = "file:///Users/red/tmp/flink/04/recursive"
        executionEnvironment.readTextFile(filePath).print()
        println("----------------------------")
        val parameters = new Configuration
        parameters.setBoolean("recursive.file.enumeration", true)
        executionEnvironment.readTextFile(filePath).withParameters(parameters).print()
    }

    def readCompressionFiles(executionEnvironment: ExecutionEnvironment): Unit = {
        val filePath = "file:///Users/red/tmp/flink/04/compression.tar.gz"
        executionEnvironment.readTextFile(filePath).print()
    }
}
