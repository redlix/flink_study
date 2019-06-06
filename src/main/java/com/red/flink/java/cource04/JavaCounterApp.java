package com.red.flink.java.cource04;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.accumulators.LongCounter;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem;

/**
 * <pre>
 *  Version         Date            Author          Description
 * ------------------------------------------------------------
 *  1.0.0           2019/06/06     red        -
 * </pre>
 *
 * @author red
 * @version 1.0.0 2019-06-06 10:54
 * @since 1.0.0
 */
public class JavaCounterApp {
	public static void main(String[] args) throws Exception {
		ExecutionEnvironment executionEnvironment = ExecutionEnvironment.getExecutionEnvironment();

		DataSource<String> data = executionEnvironment.fromElements("hadoop", "spark", "flink", "pyspark", "storm");

		DataSet<String> info = data.map(new RichMapFunction<String, String>() {
			// step1: 定义计数器
			LongCounter counter = new LongCounter();

			@Override
			public void open(Configuration parameters) throws Exception {
				// step2: 注册 计数器
				getRuntimeContext().addAccumulator("element_counts_java", counter);
			}

			@Override
			public String map(String s) throws Exception {
				counter.add(1);
				return s;
			}
		});

		String filePath = "file:///Users/red/tmp/flink/04/java_counter_sink_out";
		info.writeAsText(filePath, FileSystem.WriteMode.OVERWRITE).setParallelism(3);
		JobExecutionResult result = executionEnvironment.execute("JavaCounterApp");
		// step3: 获取计数器
		long num = result.getAccumulatorResult("element_counts_java");
		System.out.println("num: " + num);
	}
}
