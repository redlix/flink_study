package com.red.flink.java.course06;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.types.Row;

/**
 * JavaTableSQLAPI
 * <pre>
 *  Version         Date            Author          Description
 * ------------------------------------------------------------
 *  1.0.0           2019/06/10     red        -
 * </pre>
 *
 * @author red
 * @version 1.0.0 2019-06-10 11:08
 * @since 1.0.0
 */
public class JavaTableSQLAPI {
	public static void main(String[] args) throws Exception {


		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		BatchTableEnvironment tableEnv = BatchTableEnvironment.create(env);

		String filePath = "file:///Users/red/tmp/flink/06/sales.csv";

		DataSet<Sales> csv = env.readCsvFile(filePath)
				.ignoreFirstLine()
				.pojoType(Sales.class, "transactionId", "customerId", "itemId", "amountPaid");
		//csv.print();

		Table sales = tableEnv.fromDataSet(csv);
		tableEnv.registerTable("sales", sales);
		Table resultTable = tableEnv
				.sqlQuery("select customerId, sum(amountPaid) money from sales group by customerId");

		DataSet<Row> result = tableEnv.toDataSet(resultTable, Row.class);
		result.print();
	}

	public static class Sales {
		public String transactionId;
		public String customerId;
		public String itemId;
		public Double amountPaid;

	}
}
