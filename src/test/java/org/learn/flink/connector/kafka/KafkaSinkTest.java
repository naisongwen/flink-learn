package org.learn.flink.connector.kafka;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkFixedPartitioner;
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkKafkaPartitioner;
import org.learn.flink.Constants;

import java.util.Properties;

public class KafkaSinkTest {

    public static void main(String[] args) throws Exception {
        final String topic = "test_density_topic";

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<String> stream = env.addSource(new SourceFunction<String>() {
            private static final long serialVersionUID = -2255105836471289626L;
            boolean running = true;
            int count = 0;

            //String sample = "{\"database\":\"test\",\"table\":\"product\",\"type\":\"insert\",\"ts\":1596684883,\"xid\":7125,\"xoffset\":0,\"data\":{\"id\":101,\"name\":\"scooter\",\"description\":\"Small 2-wheel scooter\",\"weight\":3.14}}\n";
            final String sample = "{\"database\":\"test\"}";
            //String sample ="non json format data";
            @Override
            public void run(SourceContext<String> ctx) throws InterruptedException {
                while (running && count <5) {
                    ctx.collectWithTimestamp(sample, System.currentTimeMillis());
                    count++;
                    Thread.sleep(1000);
                }
            }

            @Override
            public void cancel() {
                running = false;
            }
        });


        SerializationSchema<String> serSchema = new SimpleStringSchema();
        FlinkKafkaPartitioner<String> partitioner = new FlinkFixedPartitioner<>();

        Properties kafkaProperties = new Properties();

        // properties have to be Strings
        kafkaProperties.put("replica.fetch.max.bytes", String.valueOf(50 * 1024 * 1024));
        kafkaProperties.put(
                "transaction.timeout.ms", Integer.toString(1000 * 60));

        stream.sinkTo(
                KafkaSink.<String>builder()
                        .setKafkaProducerConfig(kafkaProperties)
                        .setBootstrapServers(Constants.kafkaServers)
                        .setRecordSerializer(
                                KafkaRecordSerializationSchema.builder()
                                        .setTopic(topic)
                                        .setValueSerializationSchema(serSchema)
                                        .setPartitioner(partitioner)
                                        .build())
                        .setDeliverGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                        .build()).setParallelism(1);

        env.execute();
    }
}
