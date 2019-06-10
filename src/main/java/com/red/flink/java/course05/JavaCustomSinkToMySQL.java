package com.red.flink.java.course05;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * <pre>
 *  Version         Date            Author          Description
 * ------------------------------------------------------------
 *  1.0.0           2019/06/10     red        -
 * </pre>
 *
 * @author red
 * @version 1.0.0 2019-06-10 09:52
 * @since 1.0.0
 */
public class JavaCustomSinkToMySQL {
	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		DataStreamSource<String> data = env.socketTextStream("localhost", 9999);

		SingleOutputStreamOperator<Student> streamOperator = data.map(new MapFunction<String, Student>() {

			@Override
			public Student map(String s) throws Exception {
				String[] splits = s.split(",");

				Student student = new Student(Integer.parseInt(splits[0]), splits[1], Integer.parseInt(splits[2]));

				return student;
			}
		});
		streamOperator.addSink(new SinkToMySQL());

		env.execute("JavaCustomSinkToMySQL");
	}
}
