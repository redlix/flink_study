package com.red.flink.java.cource04;

import com.red.flink.scala.cource04.DbUtil;
import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;

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
 * @version 1.0.0 2019-06-06 09:25
 * @since 1.0.0
 */
public class JavaDataSetTransformationApp {
	public static void main(String[] args) throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

//		crossJoin(env);
//		outerJoinFunction(env);
//		joinFunction(env);
//		distinctFunction(env);
//		flatMapFunction(env);
//		firstFunction(env);
//		mapPartitionFunction(env);
//		filterFunction(env);
		mapFunction(env);
	}

	private static void crossJoin(ExecutionEnvironment environment) throws Exception {
		List<String> info1 = new ArrayList<>();
		info1.add("曼联");
		info1.add("曼城");

		List<Integer> info2 = new ArrayList<>();
		info2.add(3);
		info2.add(1);
		info2.add(0);

		DataSource<String> data1 = environment.fromCollection(info1);
		DataSource<Integer> data2 = environment.fromCollection(info2);

		data1.cross(data2).print();
	}

	private static void outerJoinFunction(ExecutionEnvironment env) throws Exception {
		List<Tuple2<Integer, String>> info1 = new ArrayList<Tuple2<Integer, String>>();
		info1.add(new Tuple2<>(1, "PK哥"));
		info1.add(new Tuple2<>(2, "J哥"));
		info1.add(new Tuple2<>(3, "小队长"));
		info1.add(new Tuple2<>(4, "猪头呼"));


		List<Tuple2<Integer, String>> info2 = new ArrayList<Tuple2<Integer, String>>();
		info2.add(new Tuple2<>(1, "北京"));
		info2.add(new Tuple2<>(2, "上海"));
		info2.add(new Tuple2<>(3, "成都"));
		info2.add(new Tuple2<>(5, "杭州"));

		DataSource<Tuple2<Integer, String>> data1 = env.fromCollection(info1);
		DataSource<Tuple2<Integer, String>> data2 = env.fromCollection(info2);

		data1.leftOuterJoin(data2).where(0).equalTo(0).with(
				new JoinFunction<Tuple2<Integer, String>, Tuple2<Integer, String>, Tuple3<Integer, String, String>>() {
					public Tuple3<Integer, String, String> join(Tuple2<Integer, String> first,
																Tuple2<Integer, String> second) {
						if (second == null) {
							return new Tuple3<>(first.f0, first.f1, "_");
						} else {
							return new Tuple3<>(first.f0, first.f1, second.f1);
						}
					}
				}).print();

//		data1.rightOuterJoin(data2).where(0).equalTo(0).with(new JoinFunction<Tuple2<Integer,String>, Tuple2<Integer,String>, Tuple3<Integer,String, String>>() {
//            public Tuple3<Integer, String, String> join(Tuple2<Integer, String> first, Tuple2<Integer, String> second) throws Exception {
//                if(first == null) {
//                    return new Tuple3<Integer, String, String>(second.f0, "-", second.f1);
//                } else {
//                    return new Tuple3<Integer, String, String>(first.f0, first.f1, second.f1);
//                }
//            }
//        }).print();


//        data1.fullOuterJoin(data2).where(0).equalTo(0).with(new JoinFunction<Tuple2<Integer, String>, Tuple2<Integer, String>, Tuple3<Integer, String, String>>() {
//				public Tuple3<Integer, String, String> join(Tuple2<Integer, String> first, Tuple2<Integer, String> second) throws Exception {
//					if (first == null) {
//						return new Tuple3<Integer, String, String>(second.f0, "-", second.f1);
//					} else if (second == null) {
//						return new Tuple3<Integer, String, String>(first.f0, first.f1, "-");
//					} else {
//						return new Tuple3<Integer, String, String>(first.f0, first.f1, second.f1);
//					}
//				}
//			}).print();

//        data1.rightOuterJoin(data2).where(0).equalTo(0).with(new JoinFunction<Tuple2<Integer,String>, Tuple2<Integer,String>, Tuple3<Integer,String, String>>() {
//            public Tuple3<Integer, String, String> join(Tuple2<Integer, String> first, Tuple2<Integer, String> second) throws Exception {
//                if(first == null) {
//                    return new Tuple3<Integer, String, String>(second.f0, "-", second.f1);
//                } else {
//                    return new Tuple3<Integer, String, String>(first.f0, first.f1, second.f1);
//                }
//            }
//        }).print();


		data1.fullOuterJoin(data2).where(0).equalTo(0)
				.with(new JoinFunction<Tuple2<Integer, String>, Tuple2<Integer, String>, Tuple3<Integer, String, String>>() {
					public Tuple3<Integer, String, String> join(Tuple2<Integer, String> first,
																Tuple2<Integer, String> second) throws Exception {
						if (first == null) {
							return new Tuple3<Integer, String, String>(second.f0, "-", second.f1);
						} else if (second == null) {
							return new Tuple3<Integer, String, String>(first.f0, first.f1, "-");
						} else {
							return new Tuple3<Integer, String, String>(first.f0, first.f1, second.f1);
						}
					}
				}).print();

	}

	private static void joinFunction(ExecutionEnvironment environment) throws Exception {
		List<Tuple2<Integer, String>> info1 = new ArrayList<Tuple2<Integer, String>>();
		info1.add(new Tuple2<>(1, "PK哥"));
		info1.add(new Tuple2<>(2, "J哥"));
		info1.add(new Tuple2<>(3, "小队长"));
		info1.add(new Tuple2<>(4, "猪头呼"));


		List<Tuple2<Integer, String>> info2 = new ArrayList<Tuple2<Integer, String>>();
		info2.add(new Tuple2<>(1, "北京"));
		info2.add(new Tuple2<>(2, "上海"));
		info2.add(new Tuple2<>(3, "成都"));
		info2.add(new Tuple2<>(5, "杭州"));

		DataSource<Tuple2<Integer, String>> data1 = environment.fromCollection(info1);
		DataSource<Tuple2<Integer, String>> data2 = environment.fromCollection(info2);

		data1.join(data2).where(0).equalTo(0).with(
				new JoinFunction<Tuple2<Integer, String>, Tuple2<Integer, String>, Tuple3<Integer, String, String>>() {
					@Override
					public Tuple3<Integer, String, String> join(Tuple2<Integer, String> first,
																Tuple2<Integer, String> second)
							throws Exception {
						return new Tuple3<Integer, String, String>(first.f0, first.f1, second.f1);
					}
				}).print();
	}

	private static void distinctFunction(ExecutionEnvironment environment) throws Exception {
		List<String> info = new ArrayList<>();
		info.add("hadoop, spark");
		info.add("hadoop, flink");
		info.add("flink, flink");

		DataSource<String> data = environment.fromCollection(info);

		data.flatMap(new FlatMapFunction<String, String>() {
			@Override
			public void flatMap(String input, Collector<String> collector) throws Exception {
				String[] splits = input.split(",");
				for (String split : splits) {
					collector.collect(split.trim());
				}
			}
		}).distinct().print();
	}

	private static void flatMapFunction(ExecutionEnvironment environment) throws Exception {
		List<String> info = new ArrayList<>();
		info.add("Hadoop, spark");
		info.add("hadoop, flink");
		info.add("flink, Flink");

		DataSource<String> data = environment.fromCollection(info);

		data.flatMap(new FlatMapFunction<String, String>() {
			@Override
			public void flatMap(String s, Collector<String> collector) throws Exception {
				String[] splits = s.split(",");
				for (String split : splits) {
					collector.collect(split.trim().toLowerCase());
				}
			}
		}).map(new MapFunction<String, Tuple2<String, Integer>>() {
			@Override
			public Tuple2<String, Integer> map(String s) throws Exception {
				return new Tuple2<>(s, 1);
			}
		}).groupBy(0).sum(1).print();
	}

	private static void firstFunction(ExecutionEnvironment environment) throws Exception {
		List<Tuple2<Integer, String>> info = new ArrayList<>();
		info.add(new Tuple2<>(1, "hadoop"));
		info.add(new Tuple2<>(1, "spark"));
		info.add(new Tuple2<>(1, "flink"));
		info.add(new Tuple2<>(2, "java"));
		info.add(new Tuple2<>(2, "springboot"));
		info.add(new Tuple2<>(3, "linux"));
		info.add(new Tuple2<>(4, "vue"));

		DataSource<Tuple2<Integer, String>> data = environment.fromCollection(info);

		data.first(3).print();
		System.out.println("-------");
		data.groupBy(0).first(2).print();
		System.out.println("-------");
		data.groupBy(0).sortGroup(1, Order.DESCENDING).first(2).print();
	}

	private static void mapPartitionFunction(ExecutionEnvironment environment) throws Exception {
		List<String> list = new ArrayList<>();
		for (int i = 0; i < 100; i++) {
			list.add("student: " + i);
		}

		DataSource<String> data = environment.fromCollection(list).setParallelism(6);

//		data.map(new MapFunction<String, String>() {
//			@Override
//			public String map(String s) throws Exception {
//				String connection = DbUtil.getConnection();
//				System.out.println("connection = [" + connection + "]");
//				DbUtil.returnConnection(connection);
//				return s;
//			}
//		}).print();

		data.mapPartition(new MapPartitionFunction<String, String>() {
			@Override
			public void mapPartition(Iterable<String> iterable, Collector<String> collector) throws Exception {
				String connection = DbUtil.getConnection();
				System.out.println("connection = [" + connection + "]");
				DbUtil.returnConnection(connection);
			}
		}).print();
	}

	private static void filterFunction(ExecutionEnvironment environment) throws Exception {
		List<Integer> list = new ArrayList<>();
		for (int i = 0; i < 10; i++) {
			list.add(i);
		}

		DataSource<Integer> data = environment.fromCollection(list);

		data.map(new MapFunction<Integer, Integer>() {

			@Override
			public Integer map(Integer integer) throws Exception {
				return integer + 1;
			}
		}).filter(new FilterFunction<Integer>() {
			@Override
			public boolean filter(Integer integer) throws Exception {
				return integer > 5;
			}
		}).print();
	}

	private static void mapFunction(ExecutionEnvironment environment) throws Exception {
		List<Integer> list = new ArrayList<>();
		for (int i = 0; i < 10; i++) {
			list.add(i);
		}
		DataSource<Integer> data = environment.fromCollection(list);

		data.map(new MapFunction<Integer, Integer>() {

			@Override
			public Integer map(Integer integer) throws Exception {
				return integer + 1;
			}
		}).print();
	}
}
