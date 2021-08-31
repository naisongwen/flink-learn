package org.learn.flink.quickstart.streaming;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.learn.flink.common.ClickEvent;

import java.sql.Timestamp;
import java.time.Duration;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.table.api.Expressions.$;

//java -cp target/flink-learn-1.0-SNAPSHOT.jar;;C:\Users\wns\.m2\org\apache\flink\flink-table-planner-blink_2.12\1.12.0\flink-table-planner-blink_2.12-1.12.0.jar  org.learn.flink.quickstart.streaming.StreamSQLExample

public class RetractStreamProcessDemo {

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
                new ClickEvent("marry", "12:30:30", "/home.html"),
                new ClickEvent("Bob", "12:45:30", "/cart.html"),
                new ClickEvent("Liz", "13:15:30", "/prod.html")
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
        });//.assignTimestampsAndWatermarks(strategy);

        mapClickEventDataStream.print();
        //如果用到rowtime，就需要assignTimestampsAndWatermarks
//        tEnv.createTemporaryView("clickEvents", mapClickEventDataStream, $("user"), $("timestamp").rowtime(), $("url"));
        tEnv.createTemporaryView("clickEvents", mapClickEventDataStream, $("user"), $("url"));
        Table groupTable = tEnv.sqlQuery("select user,count(url) as cnt from clickEvents group by user");
        DataStream<Tuple2<Boolean, Row>> retractStreamTableResult = tEnv.toRetractStream(groupTable, Row.class);
        retractStreamTableResult.print();
        String outputPath="file:///C:/Users/wns/Documents/workplace/flink-learn-java/tmp/output/group_output";
        mapClickEventDataStream.writeAsText(outputPath);
        env.execute("stream job demo");
//        final JobClient jobClient = env.executeAsync();
//        final JobExecutionResult jobExecutionResult = jobClient.getJobExecutionResult().get();
//        System.out.println(jobExecutionResult);
    }
}
