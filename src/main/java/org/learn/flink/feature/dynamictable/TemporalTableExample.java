package org.learn.flink.feature.dynamictable;

import com.google.common.collect.Lists;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.sql.Timestamp;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.TimeUnit;


public class TemporalTableExample {
    public static void main(String[] args){
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        env.setParallelism(1);
        //默认200ms
        env.getConfig().setAutoWatermarkInterval(200);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        List<Tuple3<Long, String, Timestamp>> orderData = Lists.newArrayList();
        orderData.add(new Tuple3<>(2L, "Euro", new Timestamp(2000L)));
//        orderData.add(new Tuple3<>(1L, "US Dollar", new Timestamp(3000L)));
//        orderData.add(new Tuple3<>(50L, "Yen", new Timestamp(4000L)));
//        orderData.add(new Tuple3<>(3L, "Euro", new Timestamp(5000L)));
//        orderData.add(new Tuple3<>(5L, "Euro", new Timestamp(9000L)));

        // https://cloud.tencent.com/developer/article/1697930
        // https://nightlies.apache.org/flink/flink-docs-release-1.14/zh/docs/dev/datastream/event-time/generating_watermarks/
        DataStream<Tuple3<Long, String, Timestamp>> orderStream = env.fromCollection(orderData);

        //https://nightlies.apache.org/flink/flink-docs-release-1.14/zh/docs/dev/table/concepts/time_attributes/#%E5%9C%A8-datastream-%E5%88%B0-table-%E8%BD%AC%E6%8D%A2%E6%97%B6%E5%AE%9A%E4%B9%89
        Table orderTable = tEnv.fromDataStream(orderStream, Schema.newBuilder()
                .columnByExpression("rowtime", "CAST(f2 AS TIMESTAMP(3))")
                .watermark("rowtime","SOURCE_WATERMARK()")
                .build()).as("amount","currency","eventtime");
//                $("amount"), $("currency"), $("eventtime").rowtime());// here we use rowtiime()
        orderTable.printSchema();
        tEnv.registerTable("Orders", orderTable);

        tEnv.toDataStream(orderTable).map(new MapFunction<Row, Tuple3<Long,String,Timestamp>>() {
                    @Override
                    public Tuple3<Long,String,Timestamp> map(Row r) throws Exception {
                        return Tuple3.of((Long)r.getField(0), (String)r.getField(1), (Timestamp)r.getField(2));
                    }
                })
                .keyBy(t->t.f1)
                .window(TumblingEventTimeWindows.of(Time.of(3, TimeUnit.SECONDS)))
                .sum(0).print();


        List<Tuple3<String, Long, Timestamp>> rateHistoryData = Lists.newArrayList();
        rateHistoryData.add(new Tuple3<>("US Dollar", 102L, new Timestamp(1000L)));
        rateHistoryData.add(new Tuple3<>("Euro", 114L, new Timestamp(1000L)));
        rateHistoryData.add(new Tuple3<>("Yen", 1L, new Timestamp(1000L)));
        rateHistoryData.add(new Tuple3<>("Euro", 116L, new Timestamp(5000L)));
        rateHistoryData.add(new Tuple3<>("Euro", 119L, new Timestamp(7000L)));

        DataStream<Tuple3<String, Long, Timestamp>> rateStream = env.fromCollection(rateHistoryData)
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                // here  <Tuple3< String, Long, Timestamp> is need for using withTimestampAssigner
                                .<Tuple3<String, Long, Timestamp>>forBoundedOutOfOrderness(Duration.ofSeconds(10))
                                .withTimestampAssigner((event, timestamp) -> event.f2.getTime())
                );

        Table rateTable = tEnv.fromDataStream(
                rateStream,
                Schema.newBuilder()
//                        .columnByMetadata("rowtime", "TIMESTAMP(3)")
                        .columnByExpression("rowtime", "CAST(f2 AS TIMESTAMP(3))")
                        .watermark("rowtime", "rowtime - INTERVAL '10' SECOND")
//                        .watermark("rowtime","SOURCE_WATERMARK()")
                        .primaryKey("f0")
                        .build()).as("currency","rate","eventtime");

        // rateTable.
        rateTable.printSchema();//only for debug
        tEnv.registerTable("RatesHistory", rateTable);
        // https://nightlies.apache.org/flink/flink-docs-release-1.14/zh/docs/dev/table/sql/queries/joins/#event-time-temporal-join
        String sqlQuery =
                "SELECT o.eventtime, o.currency, o.amount, r.rate, " +
                        " o.amount * r.rate as amount_sum " +
                        "from Orders AS o " +
                        "JOIN RatesHistory FOR SYSTEM_TIME AS OF o.rowtime AS r " +
                        "ON r.currency = o.currency";
        tEnv.sqlQuery(sqlQuery).execute().print();
    }
}
