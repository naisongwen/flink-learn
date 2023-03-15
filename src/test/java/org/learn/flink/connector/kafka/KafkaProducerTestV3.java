package org.learn.flink.connector.kafka;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.serialization.TypeInformationSerializationSchema;
import org.apache.flink.api.common.state.CheckpointListener;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.operators.StreamSink;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkFixedPartitioner;
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkKafkaPartitioner;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.util.InstantiationUtil;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.*;
import org.junit.rules.TemporaryFolder;
import org.learn.flink.connector.DockerImageVersions;
import org.learn.flink.connector.SharedObjects;
import org.learn.flink.connector.SharedReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.utility.DockerImageName;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.Assert.fail;

//from KafkaProducerTestBase
public class KafkaProducerTestV3 {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaProducerTestV3.class);
    public static KafkaTestEnvironment kafkaServer;
    public static Properties standardProps;
    public static String brokerConnectionStrings;
    public static Properties secureProps = new Properties();


    @BeforeClass
    public static void prepare() throws Exception {
        KafkaTestEnvironment.Config config=KafkaTestEnvironment.createConfig()
                .setKafkaServersNumber(3)
                .setSecureMode(false)
                .setHideKafkaBehindProxy(true);
        startClusters(config);
    }

    public static void startClusters(KafkaTestEnvironment.Config environmentConfig)
            throws Exception {
        kafkaServer = constructKafkaTestEnvionment();

        LOG.info("Starting KafkaTestBase.prepare() for Kafka " + kafkaServer.getVersion());

        kafkaServer.prepare(environmentConfig);

        standardProps = kafkaServer.getStandardProperties();

        brokerConnectionStrings = kafkaServer.getBrokerConnectionString();

        if (environmentConfig.isSecureMode()) {
            if (!kafkaServer.isSecureRunSupported()) {
                throw new IllegalStateException(
                        "Attempting to test in secure mode but secure mode not supported by the KafkaTestEnvironment.");
            }
            secureProps = kafkaServer.getSecureProperties();
        }
        ((KafkaTestEnvironmentImpl) kafkaServer)
                .setProducerSemantic(FlinkKafkaProducer.Semantic.EXACTLY_ONCE);
    }

    public static KafkaTestEnvironment constructKafkaTestEnvionment() throws Exception {
        Class<?> clazz =
                Class.forName(
                        KafkaTestEnvironmentImpl.class.getCanonicalName());
        return (KafkaTestEnvironment) InstantiationUtil.instantiate(clazz);
    }

    public static void shutdownClusters() throws Exception {
        if (secureProps != null) {
            secureProps.clear();
        }

        if (kafkaServer != null) {
            kafkaServer.shutdown();
        }
    }


    /** Tests the exactly-once semantic for the simple writes into Kafka. */
    @Test
    public void testExactlyOnceCustomOperator() throws Exception {
        testExactlyOnce(false, 1);
    }

    /**
     * This test sets KafkaProducer so that it will automatically flush the data and and fails the
     * broker to check whether flushed records since last checkpoint were not duplicated.
     */
    protected void testExactlyOnce(boolean regularSink, int sinksCount) throws Exception {
        final String topic =
                (regularSink ? "exactlyOnceTopicRegularSink" : "exactlyTopicCustomOperator")
                        + sinksCount;
        final int partition = 0;
        final int numElements = 1000;
        final int failAfterElements = 333;

        for (int i = 0; i < sinksCount; i++) {
            createTestTopic(topic + i, 1, 1);
        }

        TypeInformationSerializationSchema<Integer> schema =
                new TypeInformationSerializationSchema<>(
                        BasicTypeInfo.INT_TYPE_INFO, new ExecutionConfig());

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(500);
        env.setParallelism(1);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(1, 0));

        Properties properties = new Properties();
        properties.putAll(standardProps);
        properties.putAll(secureProps);

        // process exactly failAfterElements number of elements and then shutdown Kafka broker and
        // fail application
        List<Integer> expectedElements = getIntegersSequence(numElements);

        DataStream<Integer> inputStream =
                env.addSource(new IntegerSource(numElements))
                        .map(new FailingIdentityMapper<Integer>(failAfterElements));

        for (int i = 0; i < sinksCount; i++) {
            FlinkKafkaPartitioner<Integer> partitioner =
                    new FlinkKafkaPartitioner<Integer>() {
                        @Override
                        public int partition(
                                Integer record,
                                byte[] key,
                                byte[] value,
                                String targetTopic,
                                int[] partitions) {
                            return partition;
                        }
                    };

            if (regularSink) {
                StreamSink<Integer> kafkaSink =
                        kafkaServer.getProducerSink(topic + i, schema, properties, partitioner);
                inputStream.addSink(kafkaSink.getUserFunction());
            } else {
                kafkaServer.produceIntoKafka(
                        inputStream, topic + i, schema, properties, partitioner);
            }
        }

        FailingIdentityMapper.failedBefore = false;
        TestUtils.tryExecute(env, "Exactly once test");

        for (int i = 0; i < sinksCount; i++) {
            // assert that before failure we successfully snapshot/flushed all expected elements
            assertExactlyOnceForTopic(properties, topic + i, expectedElements);
            deleteTestTopic(topic + i);
        }
    }

    private List<Integer> getIntegersSequence(int size) {
        List<Integer> result = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            result.add(i);
        }
        return result;
    }

    public static void createTestTopic(
            String topic, int numberOfPartitions, int replicationFactor) {
        kafkaServer.createTestTopic(topic, numberOfPartitions, replicationFactor);
    }

    public static void deleteTestTopic(String topic) {
        kafkaServer.deleteTestTopic(topic);
    }

    private static final class InfiniteIntegerSource implements SourceFunction<Integer> {

        private volatile boolean running = true;
        private int counter = 0;

        @Override
        public void run(SourceContext<Integer> ctx) throws Exception {
            while (running) {
                ctx.collect(counter++);
            }
        }

        @Override
        public void cancel() {
            running = false;
        }
    }

    public void assertExactlyOnceForTopic(
            Properties properties, String topic, List<Integer> expectedElements) {

        List<Integer> actualElements = new ArrayList<>();

        Properties consumerProperties = new Properties();
        consumerProperties.putAll(properties);
        consumerProperties.put(
                "key.deserializer", "org.apache.kafka.common.serialization.IntegerDeserializer");
        consumerProperties.put(
                "value.deserializer", "org.apache.kafka.common.serialization.IntegerDeserializer");
        consumerProperties.put("isolation.level", "read_committed");

        // query kafka for new records ...
        Collection<ConsumerRecord<byte[], byte[]>> records =
                kafkaServer.getAllRecordsFromTopic(consumerProperties, topic);

        for (ConsumerRecord<byte[], byte[]> record : records) {
            actualElements.add(ByteBuffer.wrap(record.value()).getInt());
        }

        // succeed if we got all expectedElements
        if (actualElements.equals(expectedElements)) {
            return;
        }

        fail(
                String.format(
                        "Expected %s, but was: %s",
                        formatElements(expectedElements), formatElements(actualElements)));
    }

    private String formatElements(List<Integer> elements) {
        if (elements.size() > 50) {
            return String.format("number of elements: <%s>", elements.size());
        } else {
            return String.format("elements: <%s>", elements);
        }
    }
}