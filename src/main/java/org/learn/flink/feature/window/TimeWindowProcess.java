package org.learn.flink.feature.window;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.learn.flink.ClickEvent;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.Date;

public class TimeWindowProcess {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        env.setParallelism(1);
        DataStreamSource<String> clickEventSource = env.readTextFile("file:///C:/Users/wns/Documents/workplace/flink-learn-java/tmp/click_input");
        SingleOutputStreamOperator<ClickEvent> clickEventSourceStream = clickEventSource.flatMap(new FlatMapFunction<String, ClickEvent>() {
            @Override
            public void flatMap(String s, Collector<ClickEvent> collector) {
                String[] infos = s.split(" ");
                if (StringUtils.isNotBlank(s)) {
                    ClickEvent clickEvent = new ClickEvent();
                    clickEvent.setUser(infos[0]);
                    String date = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date());
                    Timestamp timestamp = Timestamp.valueOf(date + " " + infos[1]);
                    clickEvent.setTimestamp(timestamp);
                    clickEvent.setUrl(infos[2]);
                    collector.collect(clickEvent);
                }
            }
        });
        //forMonotonousTimestamps是BoundedOutOfOrdernessWatermarks类的一个子类,延迟时间 0
        WatermarkStrategy<ClickEvent> strategy = WatermarkStrategy
                //最大的延迟时间
                .<ClickEvent>forBoundedOutOfOrderness(Duration.ofSeconds(10))
                .withTimestampAssigner((event, timestamp) -> event.getTimestamp().getTime())
                //WatermarkStrategy.withIdleness()方法允许用户在配置的时间内（即超时时间内）没有记录到达时将一个流标记为空闲。
                .withIdleness(Duration.ofMinutes(1));

        DataStream<ClickEvent> withTimestampsAndWatermarks =
                clickEventSourceStream.assignTimestampsAndWatermarks(strategy);

        DataStream<Tuple3<String, String, Integer>> countResultStream = withTimestampsAndWatermarks
                .keyBy(x -> x.getUser())
                .window(TumblingEventTimeWindows.of(Time.hours(1)))
                .process(new MyCountWindowFunc());

        countResultStream.print();
        countResultStream.writeAsText("file:///C:/Users/wns/Documents/workplace/flink-learn-java/tmp/click_output.txt", FileSystem.WriteMode.OVERWRITE);
        env.execute("readTextFile");
    }

    public static class MyCountWindowFunc extends ProcessWindowFunction<
            ClickEvent,                  // input type
            Tuple3<String, String, Integer>, // output type
            String,                         // key type
            TimeWindow> {                   // window type

        @Override
        public void process(
                String key,
                Context context,
                Iterable<ClickEvent> events,
                Collector<Tuple3<String, String, Integer>> out) {
            int count = 0;
            for (ClickEvent event : events) {
                count++;
            }
            String windowEnd = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Timestamp(context.window().getEnd()));
            out.collect(Tuple3.of(windowEnd, key, count));
        }
    }
}
