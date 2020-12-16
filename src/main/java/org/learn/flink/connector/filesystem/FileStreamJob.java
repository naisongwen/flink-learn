package org.learn.flink.connector.filesystem;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.learn.flink.connector.ClickEvent;

import javax.annotation.Nullable;
import java.sql.Time;
import java.sql.Timestamp;

import static org.apache.flink.table.api.Expressions.$;

//java -cp target/flink-learn-1.0-SNAPSHOT.jar;C:\Users\wns\.m2\org\apache\flink\flink-table-planner-blink_2.12\1.12.0\flink-table-planner-blink_2.12-1.12.0.jar  org.learn.flink.connector.filesystem.FileStreamJob

public class FileStreamJob {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        EnvironmentSettings settings = EnvironmentSettings.newInstance()
//                .useBlinkPlanner()org.apache.flink.table.planner.delegation.BlinkExecutorFactory
//                .inStreamingMode()
//                .build();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        DataStreamSource<String> clickEventSource = env.readTextFile("file:///C:/Users/wns/Documents/workplace/flink-learn-java/tmp/click_input");

        //处理输入数据流，转换为StudentInfo类型，方便后续处理
        SingleOutputStreamOperator<ClickEvent> clickEventSourceStream = clickEventSource.flatMap(new FlatMapFunction<String, ClickEvent>() {
            @Override
            public void flatMap(String s, Collector<ClickEvent> collector) {
                String infos[] = s.split(" ");
                if (StringUtils.isNotBlank(s)) {
                    ClickEvent clickEvent = new ClickEvent();
                    clickEvent.setUser(infos[0]);
                    clickEvent.setCtime(Time.valueOf(infos[1]));
                    clickEvent.setUrl(infos[2]);
                    collector.collect(clickEvent);
                }
            }
        });

        DataStream<ClickEvent> dataStream = clickEventSourceStream.assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks<ClickEvent>() {
            private final long maxTimeLag = 5000; // 5 seconds

            @Nullable
            @Override
            public Watermark getCurrentWatermark() {
                return new Watermark(System.currentTimeMillis() - maxTimeLag);
            }

            @Override
            public long extractTimestamp(ClickEvent clickEvent, long l) {
                return clickEvent.getCtime().getTime();
            }
        });

        //注册dataStreamStudent流到表中，表名为：studentInfo
        tEnv.createTemporaryView("clickEvents", dataStream, $("user"), $("ctime"), $("url"));

        tEnv.registerFunction("utc2local", new UTC2Local());
        Table queryTable = tEnv.sqlQuery("select * from clickEvents");
        DataStream<Row> simpleRowStream = tEnv.toAppendStream(queryTable, Row.class);
        simpleRowStream.print();
        //Exception in thread "main" org.apache.flink.table.planner.codegen.CodeGenException:
        //Unsupported call: TUMBLE_END(TIME(0), INTERVAL SECOND(3) NOT NULL)
        Table groupWindAggrTable = tEnv.sqlQuery("select user,tumble_end(ctime,interval '1' hours) as end_time,count(url) as cnt\n" +
                "from clickEvents\n" +
                "group by user,tumble_end(ctime,interval '1' hours)");
        DataStream<Tuple2<Boolean, Row>> groupWindAggrTableResult = tEnv.toRetractStream(groupWindAggrTable, Row.class);
        groupWindAggrTableResult.print();

        env.execute("clickEventAnalyseJob");
    }

    static public class UTC2Local extends ScalarFunction {
        public Timestamp eval(Timestamp s) {
            long timestamp = s.getTime() + 28800000;
            //flink默认的是UTC时间，我们的时区是东八区，时间戳需要增加八个小时
            return new Timestamp(timestamp);
        }
    }
}
