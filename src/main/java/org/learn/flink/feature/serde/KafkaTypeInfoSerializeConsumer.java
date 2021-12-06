package org.learn.flink.feature.serde;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.TypeInformationSerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.learn.flink.ClickEvent;
import org.learn.flink.connector.kafka.KafkaConfig;

public class KafkaTypeInfoSerializeConsumer {
    public static void main(String[] args) throws Exception {
        final String topic = "dcttest";

        // ---------- Produce an event time stream into Kafka -------------------
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.getConfig().setRestartStrategy(RestartStrategies.noRestart());
        //设置周期性的产生水位线的时间间隔。当数据流很大的时候，如果每个事件都产生水位线，会影响性能。
        env.getConfig().setAutoWatermarkInterval(100);//默认100毫秒
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);

        //默认提供了 KafkaDeserializationSchema、JsonDeserializationSchema、TypeInformationSerializationSchema

        //TypeInformation<Tuple2<String, Double>> info = TypeInformation.of(new TypeHint<Tuple2<String, Double>>(){});
        System.out.println(TypeInformation.of(ClickEvent.class).createSerializer(new ExecutionConfig()));

        DeserializationSchema serializationSchema = new TypeInformationSerializationSchema<String>(
                TypeInformation.of(new TypeHint<String>() {
                }),
                env.getConfig()
        );

        //createKafkaSource、.setDeserializer(KafkaRecordDeserializationSchema.valueOnly(StringDeserializer.class))
        KafkaSource<String> kafkaSource =
                KafkaSource.<String>builder()
                        .setBootstrapServers(KafkaConfig.servers)
                        .setGroupId("testTimestampAndWatermark")
                        .setTopics(topic)
                        .setDeserializer(KafkaRecordDeserializationSchema.valueOnly(serializationSchema))
                        .setStartingOffsets(OffsetsInitializer.earliest())
                        .setBounded(OffsetsInitializer.latest())
                        .build();

        //KafkaSourceFunction、KafkaDynamicSource

        DataStream<String> stream = env.fromSource(
                kafkaSource,
                WatermarkStrategy.noWatermarks(),
                "testBasicReadWithoutGroupId");
        stream.print();
        env.execute("Consume again");
    }
}
