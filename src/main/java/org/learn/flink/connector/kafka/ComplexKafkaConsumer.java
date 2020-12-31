package org.learn.flink.connector.kafka;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.TypeInformationSerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.typeutils.GenericTypeInfo;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.operators.StreamSink;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.time.Duration;
import java.util.Iterator;
import java.util.Properties;

public class ComplexKafkaConsumer {
    public static void main(String[] args) throws Exception {
        final String topic = "flink-topic-test";

        // ---------- Produce an event time stream into Kafka -------------------
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.getConfig().setRestartStrategy(RestartStrategies.noRestart());
        //设置周期性的产生水位线的时间间隔。当数据流很大的时候，如果每个事件都产生水位线，会影响性能。
        env.getConfig().setAutoWatermarkInterval(100);//默认100毫秒
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        Properties properties = KafkaUtil.buildKafkaProperties(true);

        //默认提供了 KafkaDeserializationSchema(序列化需要自己编写)、JsonDeserializationSchema、AvroDeserializationSchema、TypeInformationSerializationSchema
        //new JSONKeyValueDeserializationSchema(true)
        TypeInformationSerializationSchema serializationSchema =
                new TypeInformationSerializationSchema<TypeRecord>(
                        TypeInformation.of(new TypeHint<TypeRecord>() {
                        }),
                        env.getConfig()
                );

        FlinkKafkaConsumer<TypeRecord> kafkaSource = new FlinkKafkaConsumer(topic, new MyDeserializer(), properties);
        kafkaSource.assignTimestampsAndWatermarks(WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(3)).withTimestampAssigner(new SerializableTimestampAssigner() {

            @Override
            public long extractTimestamp(Object element, long recordTimestamp) {
                return recordTimestamp;
            }
        }));

        DataStream<TypeRecord> consumeStream = env.addSource(kafkaSource);
//        consumeStream.keyBy(new KeySelector<TypeRecord, String>() {
//                                @Override
//                                public String getKey(TypeRecord value) throws Exception {
//                                    return value.boyName;
//                                }
//                            }
//        ).window(TumblingEventTimeWindows.of(Time.seconds(1))) //设置时间窗口
//                .reduce(new MyReduceFunction(), new MyProcessWindows())
//                .process(new MyProcessAllWindows())
//                .print();
        consumeStream.windowAll(TumblingEventTimeWindows.of(Time.seconds(1))) //设置时间窗口
                .process(new MyProcessAllWindows())
                .print();
        GenericTypeInfo<Object> objectTypeInfo = new GenericTypeInfo<>(Object.class);
        consumeStream.transform("timestamp validating operator", objectTypeInfo, new TimestampValidatingOperator()).setParallelism(1);

        env.execute("Consume again");
    }

    //用于如何处理窗口中的数据，即：找到窗口内按键的记录。
    static class MyReduceFunction implements ReduceFunction<TypeRecord> {
        @Override
        public TypeRecord reduce(TypeRecord value1, TypeRecord value2) {
            // 找到通话时间最长的通话记录
            return value1.key >= value2.key ? value1 : value2;
        }
    }

    //窗口处理完成后，输出的结果是什么
    static class MyProcessWindows extends ProcessWindowFunction<TypeRecord, String, String, TimeWindow> {
        @Override
        public void process(String key, ProcessWindowFunction<TypeRecord, String, String, TimeWindow>.Context context,
                            Iterable<TypeRecord> elements, Collector<String> out) {
            TypeRecord maxRecord = elements.iterator().next();
            StringBuffer sb = new StringBuffer();
            sb.append("Window Range:").append(context.window().getStart()).append("----").append(context.window().getEnd()).append("\n");
            sb.append("Window key：").append(key).append("\t");
            sb.append("Play boy：").append(maxRecord.boyName).append("\t")
                    .append("Max Key：").append(maxRecord.key).append("\n");
            out.collect(sb.toString());

//            Window Range:1609213421000----1609213422000
//            Window key：boy-D	Play boy：boy-D	Max Key：Z
//
//            Window Range:1609213421000----1609213422000
//            Window key：boy-A	Play boy：boy-A	Max Key：Z
//
//            Window Range:1609213421000----1609213422000
//            Window key：boy-E	Play boy：boy-E	Max Key：Z
//
//            Window Range:1609213421000----1609213422000
//            Window key：boy-C	Play boy：boy-C	Max Key：Z
//
//            Window Range:1609213421000----1609213422000
//            Window key：boy-B	Play boy：boy-B	Max Key：Z

        }
    }

    static class MyProcessAllWindows extends ProcessAllWindowFunction<TypeRecord, String,TimeWindow> {

        @Override
        public void process(Context context, Iterable<TypeRecord> elements, Collector<String> out) {
            Iterator<TypeRecord> iterator = elements.iterator();
            TypeRecord maxRecord = null;
            while (iterator.hasNext()) {
                TypeRecord record = iterator.next();
                if (maxRecord == null) {
                    maxRecord = record;
                    continue;
                }
                if (record.key > maxRecord.key)
                    maxRecord = record;
            }
            StringBuffer sb = new StringBuffer();
            sb.append("Window Range:").append(context.window().getStart()).append("----").append(context.window().getEnd()).append("\n");
            sb.append("Play boy：").append(maxRecord.boyName).append("\t")
                    .append("Max Key：").append(maxRecord.key).append("\n");
            out.collect(sb.toString());
        }
    }

    private static class TimestampValidatingOperator extends StreamSink<TypeRecord> {

        private static final long serialVersionUID = 1353168781235526806L;

        public TimestampValidatingOperator() {
            super(new SinkFunction<TypeRecord>() {
                private static final long serialVersionUID = -6676565693361786524L;

                @Override
                public void invoke(TypeRecord value) throws Exception {
                    throw new RuntimeException("Unexpected");
                }
            });
        }

        long elCount = 0;
        long wmCount = 0;
        long lastWM = Long.MIN_VALUE;

        @Override
        public void processElement(StreamRecord<TypeRecord> element) {
            elCount++;
        }

        @Override
        public void processWatermark(Watermark mark) {
            wmCount++;
            if (lastWM <= mark.getTimestamp()) {
                lastWM = mark.getTimestamp();
            } else {
                throw new RuntimeException("Received watermark higher than the last one");
            }
        }

        @Override
        public void close() throws Exception {
            super.close();

            if (wmCount <= 2) {
                throw new RuntimeException("Almost no watermarks have been sent " + wmCount);
            }
        }
    }

    private static class MyDeserializer implements KafkaDeserializationSchema<TypeRecord> {

        private static final long serialVersionUID = 6966177118923713521L;
        private final TypeInformation<TypeRecord> ti;
        private final TypeSerializer<TypeRecord> ser;
        long cnt = 0;

        public MyDeserializer() {
            this.ti = Types.GENERIC(TypeRecord.class);
            this.ser = ti.createSerializer(new ExecutionConfig());
        }

        @Override
        public TypeInformation<TypeRecord> getProducedType() {
            return ti;
        }

        @Override
        public TypeRecord deserialize(ConsumerRecord<byte[], byte[]> record) throws IOException {
            cnt++;
            DataInputView in = new DataInputViewStreamWrapper(new ByteArrayInputStream(record.value()));
            TypeRecord e = ser.deserialize(in);
            return e;
        }

        @Override
        public boolean isEndOfStream(TypeRecord nextElement) {
            return false;
        }
    }
}
