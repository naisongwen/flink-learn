//package org.learn.flink.connector.kafka;
//
//import org.apache.flink.api.common.restartstrategy.RestartStrategies;
//import org.apache.flink.api.common.serialization.TypeInformationSerializationSchema;
//import org.apache.flink.api.common.typeinfo.Types;
//import org.apache.flink.streaming.api.TimeCharacteristic;
//import org.apache.flink.streaming.api.datastream.DataStream;
//import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
//import org.apache.flink.streaming.api.functions.source.SourceFunction;
//import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
//import org.apache.flink.streaming.connectors.kafka.internals.KeyedSerializationSchemaWrapper;
//
//import java.util.Properties;
//import java.util.Random;
//
//public class ComplexKafkaProducer {
//    static String[] boys = new String[]{"boy-A", "boy-B", "boy-C", "boy-D", "boy-E"};
//
//    public static void main(String[] args) throws Exception {
//        final String topic = "flink-topic-test";
//
//        // ---------- Produce an event time stream into Kafka -------------------
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.setParallelism(1);
//        env.getConfig().setRestartStrategy(RestartStrategies.noRestart());
//        //设置周期性的产生水位线的时间间隔。当数据流很大的时候，如果每个事件都产生水位线，会影响性能。
//        env.getConfig().setAutoWatermarkInterval(100);//默认100毫秒
//        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
//
//        DataStream<TypeRecord> produceStream = env.addSource(new SourceFunction<TypeRecord>() {
//            private static final long serialVersionUID = -2255105836471289626L;
//            boolean running = true;
//
//            @Override
//            public void run(SourceContext<TypeRecord> ctx) {
//                long i = 0;
//                Random random = new Random();
//                while (running) {
//                    TypeRecord typeRecords = new TypeRecord(boys[random.nextInt(5)], (char) (65 + random.nextInt(26)));
//                    ctx.collectWithTimestamp(typeRecords,i * 5);
//                    if (i++ > 1000) {
//                        running = false;
//                    }
//                    try {
//                        Thread.sleep(5);
//                    } catch (InterruptedException e) {
//                        e.printStackTrace();
//                    }
//                }
//            }
//
//            @Override
//            public void cancel() {
//                running = false;
//            }
//        });
//
//        Properties properties = KafkaUtil.buildKafkaProperties(true);
//
//        final TypeInformationSerializationSchema<TypeRecord> typeSchema = new TypeInformationSerializationSchema<TypeRecord>(Types.GENERIC(TypeRecord.class), env.getConfig());
//
//        FlinkKafkaProducer<TypeRecord> myProducer = new FlinkKafkaProducer<>(
//                topic,
//                new KeyedSerializationSchemaWrapper<>(typeSchema),    // serialization schema
//                properties,
//                FlinkKafkaProducer.Semantic.EXACTLY_ONCE);
//        produceStream.addSink(myProducer);
//        env.execute("Produce some");
//    }
//}
