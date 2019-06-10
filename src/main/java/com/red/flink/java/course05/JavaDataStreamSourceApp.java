package com.red.flink.java.course05;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * <pre>
 *  Version         Date            Author          Description
 * ------------------------------------------------------------
 *  1.0.0           2019/06/10     red        -
 * </pre>
 *
 * @author red
 * @version 1.0.0 2019-06-10 08:53
 * @since 1.0.0
 */
public class JavaDataStreamSourceApp {
	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

//		socketFunction(env);
//		nonParallelSourceFunction(env);
//		parallelSourceFunction(env);
		richParallelSourceFunction(env);
		env.execute("JavaDataStreamSourceApp");
	}

	private static void richParallelSourceFunction(StreamExecutionEnvironment env) {
		DataStreamSource<Long> data = env.addSource(new JavaCustomRichParallelSourceFunction()).setParallelism(2);
		data.print();
	}

	private static void parallelSourceFunction(StreamExecutionEnvironment env) {
		DataStreamSource<Long> data = env.addSource(new JavaCustomParallelSourceFunction()).setParallelism(2);
		data.print().setParallelism(2);
	}

	private static void nonParallelSourceFunction(StreamExecutionEnvironment env) {
		DataStreamSource<Long> data = env.addSource(new JavaNonCustomParallelSourceFunction());
		data.print().setParallelism(1);
	}

	private static void socketFunction(StreamExecutionEnvironment environment) {
		DataStreamSource<String> data = environment.socketTextStream("localhost", 9999);
		data.print().setParallelism(1);
	}
}
