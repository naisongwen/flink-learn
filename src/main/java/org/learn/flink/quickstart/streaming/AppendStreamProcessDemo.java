package org.learn.flink.quickstart.streaming;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.learn.flink.common.ClickEvent;

import java.sql.Timestamp;
import java.time.Duration;
import java.util.Date;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.table.api.Expressions.$;

//java -cp target/flink-learn-1.0-SNAPSHOT.jar;;C:\Users\wns\.m2\org\apache\flink\flink-table-planner-blink_2.12\1.12.0\flink-table-planner-blink_2.12-1.12.0.jar  org.learn.flink.quickstart.streaming.StreamSQLExample

public class AppendStreamProcessDemo {

    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置checkpoint
        env.enableCheckpointing(TimeUnit.SECONDS.toMillis(10));
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        DataStream<ClickEvent> clickEventDataStream = env.fromElements(
                new ClickEvent("marry", "12:30:30", "/home.html"),
                new ClickEvent("Bob", "12:45:30", "/cart.html"),
                new ClickEvent("Liz", "13:15:30", "/prod.html"),
                new ClickEvent("marry", "13:30:30", "/home.html"),
                new ClickEvent("Bob", "14:45:30", "/cart.html"),
                new ClickEvent("Liz", "14:15:30", "/prod.html")
        );
        WatermarkStrategy<ClickEvent> strategy = WatermarkStrategy
                //最大的延迟时间
                .<ClickEvent>forBoundedOutOfOrderness(Duration.ofSeconds(10))
                .withTimestampAssigner((event, timestamp) -> event.getTimestamp().getTime());

        DataStream<ClickEvent> mapClickEventDataStream = clickEventDataStream.map(new MapFunction<ClickEvent, ClickEvent>() {
            @Override
            public ClickEvent map(ClickEvent clickEvent) {
                clickEvent.setTimestamp(Timestamp.valueOf("2020-12-16 " + clickEvent.getTime()));
                return clickEvent;
            }
        }).assignTimestampsAndWatermarks(strategy);
        mapClickEventDataStream.print();
        DataStream<Tuple4<String,Integer,Date,Date>> dataStream = mapClickEventDataStream.keyBy(new KeySelector<ClickEvent, String>() {
            @Override
            public String getKey(ClickEvent value) {
                return value.getUser();
            }
        }).window(TumblingEventTimeWindows.of(Time.hours(1))).process(new MyCountWindowFunc());

        tEnv.createTemporaryView("clickResult", dataStream, $("user"), $("cnt"),$("start"),$("end"));
        Table groupTable = tEnv.sqlQuery("select * from clickResult");
        DataStream<Row> appendStreamTableResult = tEnv.toAppendStream(groupTable, Row.class);
        appendStreamTableResult.print();
        env.execute("stream job demo");
    }

    public static class MyCountWindowFunc extends ProcessWindowFunction<
            ClickEvent,// input type
            Tuple4<String,Integer,Date,Date>, // output type
            String,// key type
            TimeWindow> {// window type

        @Override
        public void process(
                String key,
                Context context,
                Iterable<ClickEvent> events,
                Collector<Tuple4<String,Integer,Date,Date>> out) {
            int count = 0;
            for (ClickEvent event : events) {
                count++;
            }
            Date winStart=new Date(context.window().getStart());
            Date winEnd=new Date(context.window().getEnd());
            out.collect(Tuple4.of(key,count,winStart,winEnd));
        }
    }
}
