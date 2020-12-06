package org.learn.flink.connector.socket;

import com.sun.tools.javac.util.StringUtils;

import java.sql.Timestamp;

public class SocketStreamJob {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        //source,这里使用socket连接获取数据
        DataStreamSource<String> textStudent = env.socketTextStream("127.0.0.1", 9999, "\n");

        //处理输入数据流，转换为StudentInfo类型，方便后续处理
        SingleOutputStreamOperator<ClickEvent> dataStreamStudent = textStudent.flatMap(new FlatMapFunction<String, StudentInfo>() {
            @Override
            public void flatMap(String s, Collector<ClickEvent> collector){
                String infos[] = s.split(",");
                if(StringUtils.isNotBlank(s) && infos.length==5){
                    ClickEvent clickEvent = new ClickEvent();
                    clickEvent.setName(infos[0]);
                    clickEvent.setSex(infos[1]);
                    clickEvent.setCourse(infos[2]);
                    clickEvent.setTimestamp(Long.parseLong(infos[4]));
                    collector.collect(studentInfo);
                }
            }
        });

        DataStream<clickEvent> dataStream = dataStreamStudent.assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks<StudentInfo>() {
            private final long maxTimeLag = 5000; // 5 seconds
            @Nullable
            @Override
            public Watermark getCurrentWatermark() {
                return new Watermark(System.currentTimeMillis() - maxTimeLag);
            }
            @Override
            public long extractTimestamp(StudentInfo studentInfo, long l) {
                return studentInfo.getTimestamp();
            }
        });

        //注册dataStreamStudent流到表中，表名为：studentInfo
        tEnv.registerDataStream("studentInfo",dataStream,"name,sex,course,score,timestamp,sysDate.rowtime");

        tEnv.registerFunction("utc2local",new UTC2Local());

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
        DataStream<Row> overWindowAggrResult = tEnv.toAppendStream(overWindowAggr, Row.class);
        overWindowAggrResult.print();

        env.execute("studentScoreAnalyse");
    }

    class UTC2Local extends ScalarFunction {

        public Timestamp eval(Timestamp s) {
            long timestamp = s.getTime() + 28800000; flink默认的是UTC时间，我们的时区是东八区，时间戳需要增加八个小时
            return new Timestamp(timestamp);
        }
    }
