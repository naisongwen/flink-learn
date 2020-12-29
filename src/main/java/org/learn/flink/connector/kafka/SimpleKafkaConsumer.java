package org.learn.flink.connector.kafka;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.typeutils.GenericTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

public class SimpleKafkaConsumer {
    public static void main(String[] args) throws Exception {
        final String topic = "flink-topic-test";
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "192.168.1.10:9092");
        properties.setProperty("group.id", "con_test");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        FlinkKafkaConsumer<String> kafkaSource = new FlinkKafkaConsumer<>(topic,new SimpleStringSchema(), properties);
        kafkaSource.setStartFromEarliest();
        DataStream<String> consumeStream = env.addSource(kafkaSource);
        GenericTypeInfo<Object> objectTypeInfo = new GenericTypeInfo<>(Object.class);
//        consumeStream.transform("timestamp validating operator", objectTypeInfo, new TimestampValidatingOperator()).setParallelism(1);
        consumeStream.print();
        env.execute("Consume again");
    }
}
