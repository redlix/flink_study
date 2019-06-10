package com.red.flink.java.course05;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.datastream.SplitStream;

import java.util.ArrayList;
import java.util.List;

/**
 * <pre>
 *  Version         Date            Author          Description
 * ------------------------------------------------------------
 *  1.0.0           2019/06/10     red        -
 * </pre>
 *
 * @author red
 * @version 1.0.0 2019-06-10 09:12
 * @since 1.0.0
 */
public class JavaDataStreamTransformationApp {
	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

//		filterFunction(env);
//		unionFunction(env);
		splitSelectFunction(env);
		env.execute("JavaDataStreamTransformationApp");
	}

	private static void splitSelectFunction(StreamExecutionEnvironment env) {
		DataStreamSource<Long> data = env.addSource(new JavaCustomParallelSourceFunction()).setParallelism(2);
		SplitStream<Long> splits = data.split(new OutputSelector<Long>() {
			@Override
			public Iterable<String> select(Long value) {
				List<String> output = new ArrayList<>();
				if (value % 2 == 0) {
					output.add("even");
				} else {
					output.add("odd");
				}
				return output;
			}
		});
		splits.select("odd").print();
	}

	private static void unionFunction(StreamExecutionEnvironment env) {
		DataStreamSource<Long> data1 = env.addSource(new JavaCustomParallelSourceFunction()).setParallelism(1);
		DataStreamSource<Long> data2 = env.addSource(new JavaCustomParallelSourceFunction()).setParallelism(2);
		data1.union(data2).print().setParallelism(1);
	}

	private static void filterFunction(StreamExecutionEnvironment env) {
		DataStreamSource<Long> data = env.addSource(new JavaCustomParallelSourceFunction());
		data.map(new MapFunction<Long, Long>() {

			@Override
			public Long map(Long value) throws Exception {
				System.out.println("value = [" + value + "]");
				return value;
			}
		}).filter(new FilterFunction<Long>() {
			@Override
			public boolean filter(Long value) throws Exception {
				return value % 2 == 0;
			}
		}).print().setParallelism(1);
	}
}
