package org.learn.flink.connector.kafka;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.TypeInformationSerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.typeutils.GenericTypeInfo;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.queryablestate.network.messages.MessageDeserializer;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.operators.StreamSink;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer010;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.flink.streaming.connectors.kafka.internals.KeyedSerializationSchemaWrapper;
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkKafkaPartitioner;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;

import javax.annotation.Nullable;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Properties;

public class KafkaJob {
    public static void main(String[] args) throws Exception {
        final String topic = "flink-topic-test";

        // ---------- Produce an event time stream into Kafka -------------------

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.getConfig().setRestartStrategy(RestartStrategies.noRestart());
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStream<Long> streamWithTimestamps = env.addSource(new SourceFunction<Long>() {
            private static final long serialVersionUID = -2255105836471289626L;
            boolean running = true;

            @Override
            public void run(SourceContext<Long> ctx) throws Exception {
                long i = 0;
                while (running) {
                    ctx.collectWithTimestamp(i, i * 2);
                    if (i++ == 1000L) {
                        running = false;
                    }
                }
            }

            @Override
            public void cancel() {
                running = false;
            }
        });

        Properties properties=buildKafkaProperties(true);

        final TypeInformationSerializationSchema<Long> longSer = new TypeInformationSerializationSchema<>(Types.LONG, env.getConfig());
        FlinkKafkaProducer010.FlinkKafkaProducer010Configuration prod = FlinkKafkaProducer010.writeToKafkaWithTimestamps(streamWithTimestamps, topic, new KeyedSerializationSchemaWrapper<>(longSer),properties,new FlinkKafkaPartitioner<Long>() {
            private static final long serialVersionUID = -6730989584364230617L;

            @Override
            public int partition(Long next, byte[] key, byte[] value, String targetTopic, int[] partitions) {
                return (int) (next % 3);
            }
        });
        prod.setParallelism(3);
        prod.setWriteTimestampToKafka(true);
        env.execute("Produce some");

        // ---------- Consume stream from Kafka -------------------

        env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.getConfig().setRestartStrategy(RestartStrategies.noRestart());
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        FlinkKafkaConsumer010<Long> kafkaSource = new FlinkKafkaConsumer010<>(topic, new LimitedLongDeserializer(),properties);
        kafkaSource.assignTimestampsAndWatermarks(new AssignerWithPunctuatedWatermarks<Long>() {
            private static final long serialVersionUID = -4834111073247835189L;

            @Nullable
            @Override
            public Watermark checkAndGetNextWatermark(Long lastElement, long extractedTimestamp) {
                if (lastElement % 10 == 0) {
                    return new Watermark(lastElement);
                }
                return null;
            }

            @Override
            public long extractTimestamp(Long element, long previousElementTimestamp) {
                return previousElementTimestamp;
            }
        });

        DataStream<Long> stream = env.addSource(kafkaSource);
        GenericTypeInfo<Object> objectTypeInfo = new GenericTypeInfo<>(Object.class);
        stream.transform("timestamp validating operator", objectTypeInfo, new TimestampValidatingOperator()).setParallelism(1);

        env.execute("Consume again");
    }

    private static class TimestampValidatingOperator extends StreamSink<Long> {

        private static final long serialVersionUID = 1353168781235526806L;

        public TimestampValidatingOperator() {
            super(new SinkFunction<Long>() {
                private static final long serialVersionUID = -6676565693361786524L;

                @Override
                public void invoke(Long value) throws Exception {
                    throw new RuntimeException("Unexpected");
                }
            });
        }

        long elCount = 0;
        long wmCount = 0;
        long lastWM = Long.MIN_VALUE;

        @Override
        public void processElement(StreamRecord<Long> element) throws Exception {
            elCount++;
            if (element.getValue() * 2 != element.getTimestamp()) {
                throw new RuntimeException("Invalid timestamp: " + element);
            }
        }

        @Override
        public void processWatermark(Watermark mark) throws Exception {
            wmCount++;

            if (lastWM <= mark.getTimestamp()) {
                lastWM = mark.getTimestamp();
            } else {
                throw new RuntimeException("Received watermark higher than the last one");
            }

            if (mark.getTimestamp() % 10 != 0 && mark.getTimestamp() != Long.MAX_VALUE) {
                throw new RuntimeException("Invalid watermark: " + mark.getTimestamp());
            }
        }

        @Override
        public void close() throws Exception {
            super.close();
            if (elCount != 1000L) {
                throw new RuntimeException("Wrong final element count " + elCount);
            }

            if (wmCount <= 2) {
                throw new RuntimeException("Almost no watermarks have been sent " + wmCount);
            }
        }
    }

    private static class LimitedLongDeserializer implements KafkaDeserializationSchema<Long> {

        private static final long serialVersionUID = 6966177118923713521L;
        private final TypeInformation<Long> ti;
        private final TypeSerializer<Long> ser;
        long cnt = 0;

        public LimitedLongDeserializer() {
            this.ti = Types.LONG;
            this.ser = ti.createSerializer(new ExecutionConfig());
        }

        @Override
        public TypeInformation<Long> getProducedType() {
            return ti;
        }

        @Override
        public Long deserialize(ConsumerRecord<byte[], byte[]> record) throws IOException {
            cnt++;
            DataInputView in = new DataInputViewStreamWrapper(new ByteArrayInputStream(record.value()));
            Long e = ser.deserialize(in);
            return e;
        }

        @Override
        public boolean isEndOfStream(Long nextElement) {
            return cnt > 1000L;
        }
    }

    static Properties buildKafkaProperties(boolean flatMessage) {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "47.110.20.43:2093");
        properties.put("group.id", "flink-demo");
        properties.put("enable.auto.commit", false);
        /****
         * auto.offset.reset值含义解释:
         * earliest
         *
         * 当各分区下有已提交的offset时，从提交的offset开始消费；无提交的offset时，从头开始消费
         *
         * latest
         *
         * 当各分区下有已提交的offset时，从提交的offset开始消费；无提交的offset时，消费新产生的该分区下的数据
         */
        properties.put("auto.offset.reset", "earliest"); // 如果没有offset则从最后的offset开始读
        properties.put("request.timeout.ms", "40000"); // 必须大于session.timeout.ms的设置
        properties.put("session.timeout.ms", "30000"); // 默认为30秒
        properties.put("security.protocol", "SASL_PLAINTEXT");
        properties.put("sasl.mechanism", "PLAIN");
//        properties.put("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"alice\" password=\"alice-secret\";");
        properties.put("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"zkadmin\" password=\"zkadmin-pwd\";");
        properties.put("max.poll.records", "10");
        properties.put("key.deserializer", StringDeserializer.class.getName());
        if (!flatMessage) {
            properties.put("value.deserializer", MessageDeserializer.class.getName());
        } else {
            properties.put("value.deserializer", StringDeserializer.class.getName());
        }
        return properties;
    }
}
