package org.learn.flink.connector.socket;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.learn.flink.connector.ClickEvent;

import javax.annotation.Nullable;
import java.sql.Timestamp;

public class SocketStreamJob {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        //StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        EnvironmentSettings envSettings = EnvironmentSettings.newInstance()
                .useBlinkPlanner()
                .inStreamingMode()
                .build();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, envSettings);

        //source,这里使用socket连接获取数据
        DataStreamSource<String> textStudent = env.socketTextStream("127.0.0.1", 9999, "\n");

        //处理输入数据流，转换为StudentInfo类型，方便后续处理
        SingleOutputStreamOperator<ClickEvent> dataStreamStudent = textStudent.flatMap(new FlatMapFunction<String, ClickEvent>() {
            @Override
            public void flatMap(String s, Collector<ClickEvent> collector) {
                String infos[] = s.split(",");
                if (StringUtils.isNotBlank(s) && infos.length == 5) {
                    ClickEvent clickEvent = new ClickEvent();
                    clickEvent.setUser(infos[0]);
                    //clickEvent.setCtime(infos[1]);
                    clickEvent.setUrl(infos[2]);
                    collector.collect(clickEvent);
                }
            }
        });

        DataStream<ClickEvent> dataStream = dataStreamStudent.assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks<ClickEvent>() {
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
        tEnv.registerDataStream("clickEvents", dataStream, "user,ctime,url");

        tEnv.registerFunction("utc2local", new UTC2Local());

        //GroupBy Window Aggregation 根据name分组，统计学科数量
        Table groupWindAggrTable = tEnv.sqlQuery("user,tumble_end(ctime,interval '1' hours) as end_time,count(url) as cnt\n" +
                "from clicks\n" +
                "group by user,tumble_end(ctime,interval '1' hours)");
        DataStream<Row> groupWindAggrTableResult = tEnv.toAppendStream(groupWindAggrTable, Row.class);
        groupWindAggrTableResult.print();

        Table overWindowAggr = tEnv.sqlQuery(
                "user,tumble_end(ctime,interval '1' hours) as end_time,count(url) as cnt\n" +
                        "from clicks\n" +
                        "group by user,tumble_end(ctime,interval '1' hours)");
//        DataStream<Row> overWindowAggrResult = tEnv.toAppendStream(overWindowAggr, Row.class);
//        overWindowAggrResult.print();

        env.execute("clickEventAnalyseJob");
    }

    static class UTC2Local extends ScalarFunction {

        public Timestamp eval(Timestamp s) {
            long timestamp = s.getTime() + 28800000;
            //flink默认的是UTC时间，我们的时区是东八区，时间戳需要增加八个小时
            return new Timestamp(timestamp);
        }
    }
}
