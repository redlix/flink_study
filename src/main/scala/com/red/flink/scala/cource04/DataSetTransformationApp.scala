package com.red.flink.scala.cource04

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.api.common.operators.Order
import scala.collection.mutable.ListBuffer

/**
  * ${desc}
  * <pre>
  * Version         Date            Author          Description
  * ------------------------------------------------------------
  *  1.0.0           2019/06/04     red        -
  * </pre>
  *
  * @author red
  * @version 1.0.0 2019-06-04 15:54
  * @since 1.0.0 
  */
object DataSetTransformationApp {
    def main(args: Array[String]): Unit = {
        val env = ExecutionEnvironment.getExecutionEnvironment
        //        mapFunction(env)
        //        filterFunction(env)
        //        mapPartitionFunction(env)
        //        firstFunction(env)
        //        flatMapFunction(env)
        //        distinctFunction(env)
        //        joinFunction(env)
        //        fullOuterJoin(env)
        crossJoin(env)
    }

    def crossJoin(executionEnvironment: ExecutionEnvironment): Unit = {
        val info1 = List("曼联", "曼城")
        val info2 = List(3, 1, 0)

        val data1 = executionEnvironment.fromCollection(info1)
        val data2 = executionEnvironment.fromCollection(info2)

        data1.cross(data2).print()
    }

    def fullOuterJoin(executionEnvironment: ExecutionEnvironment): Unit = {
        val info1 = ListBuffer[(Int, String)]() // 编号  名字
        info1.append((1, "PK哥"))
        info1.append((2, "J哥"))
        info1.append((3, "小队长"))
        info1.append((4, "猪头呼"))

        val info2 = ListBuffer[(Int, String)]() // 编号  城市
        info2.append((1, "北京"))
        info2.append((2, "上海"))
        info2.append((3, "成都"))
        info2.append((5, "杭州"))

        val data1 = executionEnvironment.fromCollection(info1)
        val data2 = executionEnvironment.fromCollection(info2)
        //    data1.leftOuterJoin(data2).where(0).equalTo(0).apply((first,second)=>{
        //
        //      if(second == null) {
        //        (first._1, first._2, "-")
        //      } else {
        //        (first._1, first._2, second._2)
        //      }
        //    }).print()

        //    data1.rightOuterJoin(data2).where(0).equalTo(0).apply((first,second)=>{
        //
        //      if(first == null) {
        //        (second._1, "-", second._2)
        //      } else {
        //        (first._1, first._2, second._2)
        //      }
        //    }).print()
        data1.fullOuterJoin(data2).where(0).equalTo(0).apply((first, second) => {
            if (first == null) {
                (second._1, "-", second._2)
            } else if (second == null) {
                (first._1, first._2, "-")
            } else {
                (first._1, first._2, second._2)
            }
        }).print()
    }

    def joinFunction(executionEnvironment: ExecutionEnvironment): Unit = {
        val info1 = ListBuffer[(Int, String)]() // 编号  名字
        info1.append((1, "PK哥"))
        info1.append((2, "J哥"))
        info1.append((3, "小队长"))
        info1.append((4, "猪头呼"))

        val info2 = ListBuffer[(Int, String)]() // 编号  城市
        info2.append((1, "北京"))
        info2.append((2, "上海"))
        info2.append((3, "成都"))
        info2.append((5, "杭州"))

        val data1 = executionEnvironment.fromCollection(info1)
        val data2 = executionEnvironment.fromCollection(info2)

        data1.join(data2).where(0).equalTo(0).apply((first, second) => {
            (first._1, first._2, second._2)
        }).print()
    }

    def distinctFunction(executionEnvironment: ExecutionEnvironment): Unit = {
        val info = ListBuffer[String]()
        info.append("hadoop, spark")
        info.append("hadoop, flink")
        info.append("flink, flink")

        val data = executionEnvironment.fromCollection(info)

        data.flatMap(_.split(",")).map(_.trim).distinct().print()
    }

    def flatMapFunction(executionEnvironment: ExecutionEnvironment): Unit = {
        val info = ListBuffer[String]()
        info.append("hadoop, spark")
        info.append("hadoop, flink")
        info.append("flink, flink")

        val data = executionEnvironment.fromCollection(info)

        data.print()
        data.map(_.split(",")).print()
        data.flatMap(_.split(",")).map(_.trim).print()
        data.flatMap(_.split(",")).map(_.trim).map((_, 1)).groupBy(0).sum(1).print()
    }

    def firstFunction(executionEnvironment: ExecutionEnvironment): Unit = {
        val info = ListBuffer[(Int, String)]()
        info.append((1, "Hadoop"))
        info.append((1, "Spark"))
        info.append((1, "Flink"))
        info.append((2, "Java"))
        info.append((2, "SpringBoot"))
        info.append((3, "Linux"))
        info.append((4, "VUE"))

        val data = executionEnvironment.fromCollection(info)

        data.groupBy(0).sortGroup(1, Order.DESCENDING).first(2).print()
    }

    def mapPartitionFunction(executionEnvironment: ExecutionEnvironment): Unit = {
        val students = new ListBuffer[String]
        for (i <- 1 to 100) {
            students.append("student: " + i)
        }
        val data = executionEnvironment.fromCollection(students).setParallelism(5)

        //        data.map(x => {
        //            val connection = DbUtil.getConnection()
        //            println(connection + "...")
        //            DbUtil.returnConnection(connection)
        //        }).print()
        data.mapPartition(x => {
            val connection = DbUtil.getConnection()
            println(connection + "...")
            DbUtil.returnConnection(connection)
            x
        }).print()
    }

    def filterFunction(executionEnvironment: ExecutionEnvironment): Unit = {
        //        val data = executionEnvironment.fromCollection(List(1, 2, 3, 4, 5, 6, 7, 8, 9, 0))
        //        data.map(_ + 1).filter(_ > 5).print()
        executionEnvironment.fromCollection(List(1, 2, 3, 4, 5, 6)).map(_ + 1).filter(_ > 5).print()
    }

    def mapFunction(executionEnvironment: ExecutionEnvironment): Unit = {
        val data = executionEnvironment.fromCollection(List(1, 2, 3, 4, 5, 6, 7, 8, 9, 0))
        // 对data每一个元素做+1操作
        data.map((x: Int) => x + 1).print()
        data.map((x) => x + 1).print()
        data.map(x => x + 1).print()
        data.map(_ + 1).print()
    }
}
