package org.learn.flink.schema;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.learn.flink.connector.kafka.KafkaConf;

import java.util.Properties;
import java.util.Random;

public class TestAvroConsumerConfluent {
    private static final Random RANDOM = new Random();

    public static void main(String[] args) throws Exception {

        Configuration configuration = new Configuration();
        configuration.setString("pipeline.name", "example-pipeline");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment()
                .enableCheckpointing(5000L);

        env.configure(configuration);

        String bootStrapServers = KafkaConf.servers;

        Properties kafkaProperties = new Properties();
        kafkaProperties.setProperty("bootstrap.servers", bootStrapServers);

        KafkaSource<Tuple2<String, Order>> source = KafkaSource.<Tuple2<String, Order>>builder()
                .setBootstrapServers(bootStrapServers)
                .setTopics("yjh_test")
                .setDeserializer(new AvroDeserialization(Integer.class, Order.class, KafkaConf.schemaRegistryUrl))
                .setProperty("commit.offsets.on.checkpoint", "true")
                .setProperty("group.id", "flink-example")
                .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST))
                .build();


        KafkaSink<Tuple2<String, PurchaseOrder>> sink = KafkaSink.<Tuple2<String, PurchaseOrder>>builder()
                .setBootstrapServers(bootStrapServers)
                .setRecordSerializer(new AvroSerialization(Integer.class, PurchaseOrder.class, "purchase-orders", KafkaConf.schemaRegistryUrl))
                .build();

        env.fromSource(source, WatermarkStrategy.noWatermarks(), "DatagenOrders")
                .map(kv -> {
                    return new Tuple2<>(kv.f0, convert(kv.f1));
                }, new TupleTypeInfo<>(TypeInformation.of(Integer.class), TypeInformation.of(PurchaseOrder.class)))
                .sinkTo(sink);

        env.execute();

    }

    private static PurchaseOrder convert(Order order) {
        PurchaseOrder purchaseOrder = new PurchaseOrder();
        purchaseOrder.id = (order.id);
        purchaseOrder.name = (order.name);
        purchaseOrder.age = (order.age);
        return purchaseOrder;
    }

    static class Order {
        int id;
        String name;
        int age;
    }

    static class PurchaseOrder {
        int id;
        String name;
        int age;
    }
}
