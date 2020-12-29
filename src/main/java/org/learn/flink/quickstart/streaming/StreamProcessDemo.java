package org.learn.flink.quickstart.streaming;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.learn.flink.ClickEvent;

import java.sql.Timestamp;
import java.util.concurrent.TimeUnit;

//java -cp target/flink-learn-1.0-SNAPSHOT.jar;;C:\Users\wns\.m2\org\apache\flink\flink-table-planner-blink_2.12\1.12.0\flink-table-planner-blink_2.12-1.12.0.jar  org.learn.flink.quickstart.streaming.StreamSQLExample

public class StreamProcessDemo {

    // *************************************************************************
    //     PROGRAM
    // *************************************************************************

    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置checkpoint
        env.enableCheckpointing(TimeUnit.SECONDS.toMillis(10));

        DataStream<ClickEvent> clickEventDataStream = env.fromElements(
                new ClickEvent("marry", "12:30:30", "/home.html"),
                new ClickEvent("Bob", "12:45:30", "/cart.html"),
                new ClickEvent("Liz", "13:15:30", "/prod.html")
        );
        DataStream<ClickEvent> mapClickEventDataStream=clickEventDataStream.map(new MapFunction<ClickEvent, ClickEvent>() {
            @Override
            public ClickEvent map(ClickEvent clickEvent) {
                clickEvent.setTimestamp(Timestamp.valueOf("2020-12-16 " + clickEvent.getTime()));
                return clickEvent;
            }
        });
        mapClickEventDataStream.print();
//        mapClickEventDataStream.writeAsText();
        //env.execute("stream job demo");
        final JobClient jobClient = env.executeAsync();

        final JobExecutionResult jobExecutionResult = jobClient.getJobExecutionResult().get();
        System.out.println(jobExecutionResult);
    }
}
