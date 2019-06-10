package com.red.flink.java.course04;

import org.apache.flink.api.java.ExecutionEnvironment;

import java.util.ArrayList;
import java.util.List;

/**
 * <pre>
 *  Version         Date            Author          Description
 * ------------------------------------------------------------
 *  1.0.0           2019/06/04     red        -
 * </pre>
 *
 * @author red
 * @version 1.0.0 2019-06-04 09:05
 * @since 1.0.0
 */
public class JavaDataSetDataSourceApp {
	public static void main(String[] args) throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.createLocalEnvironment();
		textFile(env);
		System.out.println("---------------------------");
		fromCollection(env);
	}

	private static void textFile(ExecutionEnvironment env) throws Exception {
		String filePath = "file:///Users/red/tmp/flink/04/hello.txt";

		env.readTextFile(filePath).print();

		System.out.println("---------------------------");

		filePath = "file:///Users/red/tmp/flink/04/inputs";
		env.readTextFile(filePath).print();
	}

	private static void fromCollection(ExecutionEnvironment env) throws Exception {
		List<Integer> list = new ArrayList<>();
		for (int i = 0; i < 10; i++) {
			list.add(i);
		}
		env.fromCollection(list).print();
	}
}
