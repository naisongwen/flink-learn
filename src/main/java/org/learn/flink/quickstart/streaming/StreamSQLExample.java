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

package org.learn.flink.quickstart.streaming;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.util.Arrays;
import java.util.Objects;

import static org.apache.flink.table.api.Expressions.$;

/**
 * Simple example for demonstrating the use of SQL on a Stream Table in Java.
 *
 * <p>Usage: <code>StreamSQLExample --planner &lt;blink|flink&gt;</code><br>
 *
 * <p>This example shows how to:
 *  - Convert DataStreams to Tables
 *  - Register a Table under a name
 *  - Run a StreamSQL query on the registered Table
 *
 */

//java -cp target/flink-learn-1.0-SNAPSHOT.jar;;C:\Users\wns\.m2\org\apache\flink\flink-table-planner-blink_2.12\1.12.0\flink-table-planner-blink_2.12-1.12.0.jar  org.learn.flink.quickstart.streaming.StreamSQLExample

public class StreamSQLExample {

	// *************************************************************************
	//     PROGRAM
	// *************************************************************************

	public static void main(String[] args) throws Exception {

		final ParameterTool params = ParameterTool.fromArgs(args);
		String planner = params.has("planner") ? params.get("planner") : "flink";

		// set up execution environment
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		StreamTableEnvironment tEnv;
		if (Objects.equals(planner, "blink")) {	// use blink planner in streaming mode
			EnvironmentSettings settings = EnvironmentSettings.newInstance()
				.useBlinkPlanner()
				.inStreamingMode()
				.build();
			tEnv = StreamTableEnvironment.create(env, settings);
		} else if (Objects.equals(planner, "flink")) {	// use flink planner in streaming mode
			tEnv = StreamTableEnvironment.create(env);
		} else {
			System.err.println("The planner is incorrect. Please run 'StreamSQLExample --planner <planner>', " +
				"where planner (it is either flink or blink, and the default is flink) indicates whether the " +
				"example uses flink planner or blink planner.");
			return;
		}

		DataStream<Order> orderA = env.fromCollection(Arrays.asList(
			new Order(1L, "beer", 3),
			new Order(1L, "diaper", 4),
			new Order(3L, "rubber", 2)));

		DataStream<Order> orderB = env.fromCollection(Arrays.asList(
			new Order(2L, "pen", 3),
			new Order(2L, "rubber", 3),
			new Order(4L, "beer", 1)));

		// convert DataStream to Table
		Table tableA = tEnv.fromDataStream(orderA, $("user"), $("product"), $("amount"));
		// register DataStream as Table
		tEnv.createTemporaryView("OrderB", orderB, $("user"), $("product"), $("amount"));

		// union the two tables
		Table result = tEnv.sqlQuery("SELECT * FROM " + tableA + " WHERE amount > 2 UNION ALL " +
						"SELECT * FROM OrderB WHERE amount < 2");

		tEnv.toAppendStream(result, Order.class).print();

		// after the table program is converted to DataStream program,
		// we must use `env.execute()` to submit the job.
		env.execute();
	}

	// *************************************************************************
	//     USER DATA TYPES
	// *************************************************************************

	/**
	 * Simple POJO.
	 */
	public static class Order {
		public Long user;
		public String product;
		public int amount;

		public Order() {
		}

		public Order(Long user, String product, int amount) {
			this.user = user;
			this.product = product;
			this.amount = amount;
		}

		@Override
		public String toString() {
			return "Order{" +
				"user=" + user +
				", product='" + product + '\'' +
				", amount=" + amount +
				'}';
		}
	}
}
