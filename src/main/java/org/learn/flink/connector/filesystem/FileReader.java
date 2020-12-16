package org.learn.flink.connector.filesystem;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.util.Collector;
import org.learn.flink.connector.ClickEvent;

import javax.annotation.Nullable;
import java.sql.Time;

public class FileReader {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

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
        dataStream.print();
        env.execute("readTextFile");
    }
}
