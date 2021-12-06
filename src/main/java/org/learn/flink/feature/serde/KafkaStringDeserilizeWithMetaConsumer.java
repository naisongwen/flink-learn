package org.learn.flink.feature.serde;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.learn.flink.connector.kafka.KafkaConfig;

import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;

public class KafkaStringDeserilizeWithMetaConsumer {
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

        TestingKafkaRecordDeserializationSchema deserializationSchema=new TestingKafkaRecordDeserializationSchema();

        KafkaSource<MetaAndValue> kafkaSource =
                KafkaSource.<MetaAndValue>builder()
                        .setBootstrapServers(KafkaConfig.servers)
                        .setGroupId("testBasicRead")
                        .setTopics(topic)
                        .setDeserializer(deserializationSchema)
                        .setStartingOffsets(OffsetsInitializer.earliest())
                        .setBounded(OffsetsInitializer.latest())
                        .build();


        DataStream<MetaAndValue> stream =
                env.fromSource(
                        kafkaSource,
                        WatermarkStrategy.noWatermarks(),
                        "testBasicReadWithoutGroupId");
        stream.print();
        env.execute("Consume again");
    }

    private static class TestingKafkaRecordDeserializationSchema
            implements KafkaRecordDeserializationSchema<MetaAndValue> {
        private static final long serialVersionUID = -3765473065594331694L;
        private transient Deserializer<String> deserializer;

        @Override
        public void deserialize(
                ConsumerRecord<byte[], byte[]> record, Collector<MetaAndValue> collector)
                throws IOException {
            deserializer = new StringDeserializer();
            collector.collect(
                    new MetaAndValue(
                            new TopicPartition(record.topic(), record.partition()),
                            deserializer.deserialize(record.topic(), record.value()),record.offset()));
        }

        @Override
        public TypeInformation<MetaAndValue> getProducedType() {
            return TypeInformation.of(MetaAndValue.class);
        }
    }

    private static class MetaAndValue implements Serializable {
        private static final long serialVersionUID = 4813439951036021779L;
        private final String tp;
        private final Long offset;
        private final String value;

        private MetaAndValue(TopicPartition tp, String value,Long offset) {
            this.tp = tp.toString();
            this.value = value;
            this.offset = offset;
        }
    }
}
