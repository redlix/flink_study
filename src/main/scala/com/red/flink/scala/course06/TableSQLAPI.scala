package com.red.flink.scala.course06

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.table.api.TableEnvironment
import org.apache.flink.table.api.scala.BatchTableEnvironment
import org.apache.flink.types.Row

/**
  * ${desc}
  * <pre>
  * Version         Date            Author          Description
  * ------------------------------------------------------------
  *  1.0.0           2019/06/10     red        -
  * </pre>
  *
  * @author red
  * @version 1.0.0 2019-06-10 10:52
  * @since 1.0.0 
  */
object TableSQLAPI {
    def main(args: Array[String]): Unit = {
        val env = ExecutionEnvironment.getExecutionEnvironment
        val tableEnv = BatchTableEnvironment.create(env)

        val filePath = "file:///Users/red/tmp/flink/06/sales.csv"

        import org.apache.flink.api.scala._

        val csv = env.readCsvFile[SalesLog](filePath, ignoreFirstLine = true)

        csv.print()

        val salesTable = tableEnv.fromDataSet(csv)

        tableEnv.registerTable("sales", salesTable)

        val resultTable = tableEnv.sqlQuery("select customerId, sum(amountPaid) money from sales group by customerId")

        tableEnv.toDataSet[Row](resultTable).print()
    }

    case class SalesLog(transactionId: String,
                        customerId: String,
                        itemId: String,
                        amountPaid: Double)

}
