package org.learn.flink.connector.kafka;

import org.apache.flink.queryablestate.network.messages.MessageDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Properties;

public class KafkaUtil {
    static Properties buildKafkaProperties(boolean flatMessage) {
        Properties properties = new Properties();
        //TCP Port
        properties.put("bootstrap.servers", "192.168.1.10:9092");
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
//        properties.put("security.protocol", "SASL_PLAINTEXT");
//        properties.put("sasl.mechanism", "PLAIN");
//        properties.put("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"admin\" password=\"admin\";");
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
