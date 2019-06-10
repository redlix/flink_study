package com.red.flink.java.course04;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.core.fs.FileSystem;

import java.util.ArrayList;
import java.util.List;

/**
 * <pre>
 *  Version         Date            Author          Description
 * ------------------------------------------------------------
 *  1.0.0           2019/06/06     red        -
 * </pre>
 *
 * @author red
 * @version 1.0.0 2019-06-06 10:28
 * @since 1.0.0
 */
public class JavaDataSetSinkApp {
	public static void main(String[] args) throws Exception {
		ExecutionEnvironment executionEnvironment = ExecutionEnvironment.getExecutionEnvironment();

		List<Integer> list = new ArrayList<>();
		for (int i = 0; i < 10; i++) {
			list.add(i);
		}

		String filePath = "file:///Users/red/tmp/flink/04/java_sink_out";

		DataSource<Integer> data = executionEnvironment.fromCollection(list).setParallelism(5);
		data.writeAsText(filePath, FileSystem.WriteMode.OVERWRITE);

		executionEnvironment.execute();
	}
}
