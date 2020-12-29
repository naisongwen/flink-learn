package org.learn.flink.connector.kafka;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import java.util.Properties;

public class SimpleKafkaProducer {
    public static void main(String[] args) throws Exception {
        final String topic = "flink-topic-test";
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "192.168.1.10:9092");
        properties.setProperty("group.id", "pro_test");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<String> produceStream = env.addSource(new SourceFunction<String>() {
            private static final long serialVersionUID = -2255105836471289626L;
            boolean running = true;

            @Override
            public void run(SourceContext<String> ctx) {
                long i = 0;
                while (running) {
                    ctx.collectWithTimestamp(String.valueOf(i), i * 2);
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


        FlinkKafkaProducer<String> myProducer = new FlinkKafkaProducer<>(topic,new SimpleStringSchema(),properties);
//        final TypeInformationSerializationSchema<String> longSer = new TypeInformationSerializationSchema<>(Types.STRING, env.getConfig());
//        FlinkKafkaProducer<String> myProducer = new FlinkKafkaProducer<>(topic, new KeyedSerializationSchemaWrapper<>(longSer),properties,FlinkKafkaProducer.Semantic.EXACTLY_ONCE);

        produceStream.addSink(myProducer);
        env.execute("Produce some");
    }
}
