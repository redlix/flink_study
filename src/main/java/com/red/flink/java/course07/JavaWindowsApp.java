package com.red.flink.java.course07;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;


/**
 * <pre>
 *  Version         Date            Author          Description
 * ------------------------------------------------------------
 *  1.0.0           2019/06/17     red        -
 * </pre>
 *
 * @author red
 * @version 1.0.0 2019-06-17 09:20
 * @since 1.0.0
 */
public class JavaWindowsApp {
	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		DataStream<String> dataStream = env.socketTextStream("localhost", 9999);

		dataStream.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
			@Override
			public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
				String tokens[] = s.toLowerCase().split(",");

				for (String token : tokens) {
					if (token.length() > 0) {
						collector.collect(new Tuple2<>(token, 1));
					}
				}
			}
		}).keyBy(0)
				.timeWindow(Time.seconds(5))
				.sum(1)
				.print()
				.setParallelism(1);

		env.execute("JavaWindowsApp");
	}
}
