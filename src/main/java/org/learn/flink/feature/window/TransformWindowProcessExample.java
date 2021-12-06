package org.learn.flink.feature.window;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.calcite.shaded.com.google.common.collect.Maps;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.serialization.JSONKeyValueDeserializationSchema;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.learn.flink.connector.kafka.KafkaConfig;

import java.util.Iterator;
import java.util.Map;
import java.util.Set;

public class TransformWindowProcessExample {
    public static void main(String[] args) throws Exception {
        final String topic = "dcttest";

        // ---------- Produce an event time stream into Kafka -------------------
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setRestartStrategy(RestartStrategies.noRestart());
        //设置周期性的产生水位线的时间间隔。当数据流很大的时候，如果每个事件都产生水位线，会影响性能。
        env.getConfig().setAutoWatermarkInterval(100);//默认100毫秒
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);

        //默认提供了 KafkaDeserializationSchema(序列化需要自己编写)、JsonDeserializationSchema、AvroDeserializationSchema、TypeInformationSerializationSchema
        KafkaSource<ObjectNode> kafkaSource =
                KafkaSource.<ObjectNode>builder()
                        .setBootstrapServers(KafkaConfig.servers)
                        .setGroupId("testTimestampAndWatermark")
                        .setTopics(topic)
                        .setDeserializer(KafkaRecordDeserializationSchema.of(new JSONKeyValueDeserializationSchema(true)))
                        .setStartingOffsets(OffsetsInitializer.earliest())
                        .setBounded(OffsetsInitializer.latest())
                        .build();

        DataStream<ObjectNode> stream = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "testTimestamp");

        SingleOutputStreamOperator<Row> rowStream = stream.transform("TRANSFORM ObjectNode TO STRING", TypeInformation.of(Row.class), new MyStreamOperator());

        rowStream.print();

        rowStream.windowAll(TumblingEventTimeWindows.of(Time.seconds(5))) //设置时间窗口
                .process(new MyProcessAllWindows())
                .print();

        env.execute(TransformWindowProcessExample.class.getName());
    }

    static class MyProcessAllWindows extends ProcessAllWindowFunction<Row, Tuple3<String, Long, Long>, TimeWindow> {

        @Override
        public void process(Context context, Iterable<Row> elements, Collector<Tuple3<String, Long, Long>> out) {
            Iterator<Row> iterator = elements.iterator();
            final Map<String, Long> statMap = Maps.newHashMap();
            while (iterator.hasNext()) {
                Row row = iterator.next();
                Set<String> fieldNames = row.getFieldNames(false);
                for (String filedName : fieldNames) {
                    Object field = row.getField(filedName);
                    if (field != null)
                        statMap.put(filedName, 1 + statMap.getOrDefault(filedName, 0L));
                }
            }
            statMap.forEach((s, count) -> out.collect(new Tuple3<>(s, count,context.window().getEnd())));
        }
    }

    private static class MyStreamOperator extends AbstractStreamOperator<Row>
            implements OneInputStreamOperator<ObjectNode, Row> {
        private static final long serialVersionUID = 1L;

        @Override
        public void processElement(StreamRecord<ObjectNode> element) {
            Row row = Row.withNames();
            row.setField("timeStamp",element.getTimestamp());
            row.setField("value",element.toString());
            output.collect(new StreamRecord(row, element.getTimestamp()));
        }
    }
}
