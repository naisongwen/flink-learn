package org.learn.flink.connector.filesystem;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.DateTimeBucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.learn.flink.ClickEvent;

import javax.annotation.Nullable;
import java.sql.Timestamp;
import java.util.concurrent.TimeUnit;

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

        SingleOutputStreamOperator<ClickEvent> clickEventSourceStream = clickEventSource.flatMap(new FlatMapFunction<String, ClickEvent>() {
            @Override
            public void flatMap(String s, Collector<ClickEvent> collector) {
                String infos[] = s.split(" ");
                if (StringUtils.isNotBlank(s)) {
                    ClickEvent clickEvent = new ClickEvent();
                    clickEvent.setUser(infos[0]);
                    Timestamp timestamp = Timestamp.valueOf("2020-12-16 " + infos[1]);
                    clickEvent.setTimestamp(timestamp);
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
                return clickEvent.getTimestamp().getTime();
            }
        });

        tEnv.createTemporaryView("clickEvents", dataStream, $("user"), $("ctime").rowtime(), $("url"));
        tEnv.registerFunction("utc2local", new UTC2Local());
        Table queryTable = tEnv.sqlQuery("select * from clickEvents");
        DataStream<Row> simpleRowStream = tEnv.toAppendStream(queryTable, Row.class);
        simpleRowStream.print();
        //TODO:
        //Exception in thread "main" org.apache.flink.table.planner.codegen.CodeGenException:
        //Unsupported call: TUMBLE_END(TIME(0), INTERVAL SECOND(3) NOT NULL)
        String groupSql = "select user,tumble_end(ctime,interval '1' hours) as end_time,count(url) as cnt\n" +
                "from clickEvents\n" +
                "group by user,tumble_end(ctime,interval '1' hours)";
        groupSql = "select user,count(url) as cnt from clickEvents group by user";
        Table groupWindAggrTable = tEnv.sqlQuery(groupSql);
//        DataStream<Row> appendStreamTableResult = tEnv.toAppendStream(groupWindAggrTable, Row.class);
        DataStream<Tuple2<Boolean, Row>> retractStreamTableResult = tEnv.toRetractStream(groupWindAggrTable, Row.class);
        retractStreamTableResult.print();

        OutputFileConfig config = OutputFileConfig
                .builder()
                .withPartPrefix("prefix")
                .withPartSuffix(".txt")
                .build();

        //https://ci.apache.org/projects/flink/flink-docs-release-1.12/zh/dev/connectors/streamfile_sink.html
        String outputPath="C:/Users/wns/Documents/workplace/flink-learn-java/tmp/output/retract_output";
        final StreamingFileSink<Tuple2<Boolean, Row>> sink = StreamingFileSink
                .forRowFormat(new Path(outputPath), new SimpleStringEncoder<Tuple2<Boolean, Row>>("UTF-8"))
                /**
                 * 设置桶分配政策
                 * DateTimeBucketAssigner--默认的桶分配政策，默认基于时间的分配器，每小时产生一个桶，格式如下yyyy-MM-dd--HH
                 * BasePathBucketAssigner ：将所有部分文件（part file）存储在基本路径中的分配器（单个全局桶）
                 */
                .withBucketAssigner(new DateTimeBucketAssigner<>())
                /**
                 * 有三种滚动政策
                 *  CheckpointRollingPolicy
                 *  DefaultRollingPolicy
                 *  OnCheckpointRollingPolicy
                 */
                .withRollingPolicy(
                        /**
                         * 滚动策略决定了写出文件的状态变化过程
                         * 1. In-progress ：当前文件正在写入中
                         * 2. Pending ：当处于 In-progress 状态的文件关闭（closed）了，就变为 Pending 状态
                         * 3. Finished ：在成功的 Checkpoint 后，Pending 状态将变为 Finished 状态
                         *
                         * 观察到的现象
                         * 1.会根据本地时间和时区，先创建桶目录
                         * 2.文件名称规则：part-<subtaskIndex>-<partFileIndex>
                         * 3.在macos中默认不显示隐藏文件，需要显示隐藏文件才能看到处于In-progress和Pending状态的文件，因为文件是按照.开头命名的
                         *
                         */
                        DefaultRollingPolicy.builder()
                                .withRolloverInterval(TimeUnit.SECONDS.toMillis(2)) //设置滚动间隔
                                .withInactivityInterval(TimeUnit.SECONDS.toMillis(1)) //设置不活动时间间隔
                                .withMaxPartSize(1024 * 1024 * 1024) // 最大零件尺寸
                                .build())
                .withOutputFileConfig(config)
                .build();

        retractStreamTableResult.addSink(sink).setParallelism(1);
//        retractStreamTableResult.writeAsText("file:///C:/Users/wns/Documents/workplace/flink-learn-java/tmp/retract_output", FileSystem.WriteMode.OVERWRITE);
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
