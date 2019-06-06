package com.red.flink.java.cource04;

import org.apache.commons.io.FileUtils;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.configuration.Configuration;

import java.io.File;
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
 * @version 1.0.0 2019-06-06 11:20
 * @since 1.0.0
 */
public class JavaDistributedCacheApp {
	public static void main(String[] args) throws Exception {
		ExecutionEnvironment environment = ExecutionEnvironment.getExecutionEnvironment();
		String filePath = "file:///Users/red/tmp/flink/04/hello.txt";

		// step1: 注册本地文件
		environment.registerCachedFile(filePath, "java_dc");

		DataSource<String> data = environment.fromElements("hadoop", "spark", "flink", "pyspark", "storm");

		data.map(new RichMapFunction<String, String>() {

			List<String> list = new ArrayList<>();

			@Override
			public void open(Configuration parameters) throws Exception {
				File file = getRuntimeContext().getDistributedCache().getFile("java_dc");
				List<String> lines = FileUtils.readLines(file);

				for (String line : lines) {
					list.add(line);
					System.out.println("line = [" + line + "]");
				}
			}


			@Override
			public String map(String s) throws Exception {
				return s;
			}
		}).print();

	}
}
