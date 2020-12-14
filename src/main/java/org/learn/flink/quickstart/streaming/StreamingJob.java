/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.learn.flink.streaming;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.wikiedits.WikipediaEditEvent;
import org.apache.flink.streaming.connectors.wikiedits.WikipediaEditsSource;

import javax.annotation.Nullable;

/**
 * Skeleton for a Flink Streaming Job.
 *
 * <p>For a tutorial how to write a Flink streaming application, check the
 * tutorials and examples on the <a href="https://flink.apache.org/docs/stable/">Flink Website</a>.
 *
 * <p>To package your application into a JAR file for execution, run
 * 'mvn clean package' on the command line.
 *
 * <p>If you change the name of the main class (with the public static void main(String[] args))
 * method, change the respective entry in the POM.xml file (simply search for 'mainClass').
 */
public class StreamingJob {

	public static void main(String[] args) throws Exception {
		//1.获取环境信息
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);


		/*
		 * Here, you can start creating your execution plan for Flink.
		 *
		 * Start with getting some data from the environment, like
		 * 	env.readTextFile(textPath);
		 *
		 * then, transform the resulting DataStream<String> using operations
		 * like
		 * 	.filter()
		 * 	.flatMap()
		 * 	.join()
		 * 	.coGroup()
		 *
		 * and many more.
		 * Have a look at the programming guide for the Java API:
		 *
		 * https://flink.apache.org/docs/latest/apis/streaming/index.html
		 *
		 */
		//2.为环境信息添加WikipediaEditsSource源
		DataStream<WikipediaEditEvent> edits = env.addSource(new WikipediaEditsSource()).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessGenerator());

		//3.根据事件中的用户名为key来区分数据流
		KeyedStream<WikipediaEditEvent, String> keyedEdits = edits
				.keyBy(new KeySelector<WikipediaEditEvent, String>() {
					@Override
					public String getKey(WikipediaEditEvent wikipediaEditEvent) throws Exception {
						return wikipediaEditEvent.getUser();
					}
				});

		DataStream<Tuple2<String, Integer>> result = keyedEdits
				//4.设置窗口时间为5s
				.timeWindow(Time.seconds(5))
				//5.聚合当前窗口中相同用户名的事件，最终返回一个tuple2<user，累加的ByteDiff>
				.aggregate(new AggregateFunction<WikipediaEditEvent, Tuple2<String, Integer>, Tuple2<String,Integer>>() {
					@Override
					public Tuple2<String, Integer> createAccumulator() {
						return new Tuple2<>("",0);
					}

					@Override
					public Tuple2<String, Integer> add(WikipediaEditEvent value, Tuple2<String, Integer> accumulator) {
						accumulator.f0=value.getUser();
						accumulator.f1=accumulator.f1+value.getByteDiff();
						return accumulator;
					}

					@Override
					public Tuple2<String, Integer> getResult(Tuple2<String, Integer> accumulator) {
						return accumulator;
					}

					@Override
					public Tuple2<String, Integer> merge(Tuple2<String, Integer> a, Tuple2<String, Integer> b) {
						a.f1=a.f1+b.f1;
						return a;
					}
				});

		result.print();//这里可能是发送到kafka中或者落地到其他的存储比如redis等等

		//https://ci.apache.org/projects/flink/flink-docs-stable/dev/table/connect.html#jdbc-connector
//		JDBCAppendTableSink sink = JDBCAppendTableSink.builder()
//				.setDrivername("org.apache.derby.jdbc.EmbeddedDriver")
//				.setDBUrl("jdbc:derby:memory:ebookshop")
//				.setQuery("INSERT INTO wiki_edits (user,count) VALUES (?,?)")
//				.setParameterTypes(STRING_TYPE_INFO,INT_TYPE_INFO)
//				.build();

//		tableEnv.registerTableSink(
//				"jdbcOutputTable",
//				// specify table schema
//				new String[]{"user,count"},
//				new TypeInformation[]{Types.STRING,Types.INT},
//				sink);
//
//		Table table = ...
//		table.insertInto("jdbcOutputTable");

		// execute program
		env.execute("Flink Streaming Java API Skeleton");
	}


	//https://ci.apache.org/projects/flink/flink-docs-release-1.10/dev/event_timestamps_watermarks.html#with-periodic-watermarks
	/**
	 * This generator generates watermarks（水位线） assuming that elements arrive out of order,
	 * but only to a certain degree. The latest elements for a certain timestamp t will arrive
	 * at most n milliseconds after the earliest elements for timestamp t.
	 */
	private static class BoundedOutOfOrdernessGenerator implements AssignerWithPeriodicWatermarks<WikipediaEditEvent> {

		//设置watermarks方式为不迟于当前最大EventTime 减去一个 固定时间
		private final long maxOutOfOrderness = 1000; // 1 seconds

		private long currentMaxTimestamp;

		@Nullable
		@Override
		public Watermark getCurrentWatermark() {
			return new Watermark(currentMaxTimestamp - maxOutOfOrderness);
		}

		@Override
		public long extractTimestamp(WikipediaEditEvent element, long previousElementTimestamp) {
			long timestamp = element.getTimestamp();
			currentMaxTimestamp = Math.max(timestamp, currentMaxTimestamp);
			return timestamp;
		}
	}
}
