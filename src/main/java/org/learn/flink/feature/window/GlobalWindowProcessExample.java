package org.learn.flink.feature.window;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.util.serialization.JSONKeyValueDeserializationSchema;
import org.apache.flink.util.Collector;
import org.learn.flink.connector.kafka.KafkaConfig;

import java.util.Iterator;

public class GlobalWindowProcessExample {
    public static void main(String[] args) throws Exception {
        final String topic = "dcttest";

        // ---------- Produce an event time stream into Kafka -------------------
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
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

        //stream.transform("TRANSFORM ObjectNode TO STRING",TypeInformation.of(ObjectNode.class),);
        stream.windowAll(TumblingEventTimeWindows.of(Time.seconds(5))) //设置时间窗口
                .process(new MyProcessAllWindows())
                .print();

        env.execute("Consume again");
    }

    static class MyProcessAllWindows extends ProcessAllWindowFunction<ObjectNode, String,TimeWindow> {

        @Override
        public void process(Context context, Iterable<ObjectNode> elements, Collector<String> out) {
            Iterator<ObjectNode> iterator = elements.iterator();
            while (iterator.hasNext()) {
                ObjectNode objectNode=iterator.next();
                out.collect(objectNode.toString());
            }
            StringBuffer sb = new StringBuffer();
            sb.append("Window Range:").append(context.window().getStart()).append("----").append(context.window().getEnd()).append("\n");
        }
    }
}
