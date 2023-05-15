package org.learn.flink.connector.kafka;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.CheckpointListener;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkFixedPartitioner;
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkKafkaPartitioner;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
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
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

//from KafkaChangelogTableITCase
//本测试用例，需要Docker环境
public class KafkaContainerTest {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaContainerTest.class);
    private static final Network NETWORK = Network.newNetwork();
    private static final String INTER_CONTAINER_KAFKA_ALIAS = "kafka";
    static final String topic = "changelog_maxwell";

    @ClassRule
    public static final KafkaContainer KAFKA_CONTAINER =
            new KafkaContainer(DockerImageName.parse(DockerImageVersions.KAFKA)) {
                @Override
                protected void doStart() {
                    super.doStart();
                    if (LOG.isInfoEnabled()) {
                        this.followOutput(new Slf4jLogConsumer(LOG));
                    }
                }
            }.withEmbeddedZookeeper()
                    .withNetwork(NETWORK)
                    .withNetworkAliases(INTER_CONTAINER_KAFKA_ALIAS)
                    .withEnv(
                            "KAFKA_TRANSACTION_MAX_TIMEOUT_MS",
                            String.valueOf(Duration.ofHours(2).toMillis()))
                    // Disable log deletion to prevent records from being deleted during test run
                    .withEnv("KAFKA_LOG_RETENTION_MS", "-1");

    private static final short TOPIC_REPLICATION_FACTOR = 1;
    private static final int zkTimeoutMills = 30000;
    private static AdminClient admin;
    @Rule
    public final SharedObjects sharedObjects = SharedObjects.create();
    @Rule
    public final TemporaryFolder temp = new TemporaryFolder();
    protected StreamExecutionEnvironment env;
    protected StreamTableEnvironment tEnv;
    private SharedReference<AtomicLong> emittedRecordsCount;
    private SharedReference<AtomicLong> emittedRecordsWithCheckpoint;
    private SharedReference<AtomicBoolean> failed;
    private SharedReference<AtomicLong> lastCheckpointedRecord;

    @BeforeClass
    public static void setupAdmin() throws ExecutionException, InterruptedException, TimeoutException {
        Map<String, Object> properties = new HashMap<>();
        properties.put(
                CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG,
                KAFKA_CONTAINER.getBootstrapServers());
        admin = AdminClient.create(properties);
        createTestTopic(topic, 1, TOPIC_REPLICATION_FACTOR);
    }

    @AfterClass
    public static void teardownAdmin() {
        admin.close();
        deleteTestTopic(topic);
    }

    @Before
    public void setUp() {
        emittedRecordsCount = sharedObjects.add(new AtomicLong());
        emittedRecordsWithCheckpoint = sharedObjects.add(new AtomicLong());
        failed = sharedObjects.add(new AtomicBoolean(false));
        lastCheckpointedRecord = sharedObjects.add(new AtomicLong(0));
        env = StreamExecutionEnvironment.getExecutionEnvironment();
        tEnv = StreamTableEnvironment.create(env);
        env.getConfig().setRestartStrategy(RestartStrategies.noRestart());
    }

    @Test
    public void testWriteRecordsToKafka() throws Exception {
        // enables MiniBatch processing to verify MiniBatch + FLIP-95, see FLINK-18769
        Configuration tableConf = tEnv.getConfig().getConfiguration();
        tableConf.setString("table.exec.mini-batch.enabled", "true");
        tableConf.setString("table.exec.mini-batch.allow-latency", "1s");
        tableConf.setString("table.exec.mini-batch.size", "5000");
        tableConf.setString("table.optimizer.agg-phase-strategy", "TWO_PHASE");

        // ---------- Write the Debezium json into Kafka -------------------
        List<String> lines = readLines("debezium-data-schema-exclude.txt");
        DataStreamSource<String> stream = env.fromCollection(lines);
        SerializationSchema<String> serSchema = new SimpleStringSchema();
        FlinkKafkaPartitioner<String> partitioner = new FlinkFixedPartitioner<>();

        // the producer must not produce duplicates
        Properties producerProperties = getStandardProps();
        producerProperties.setProperty("retries", "0");

        stream.sinkTo(
                KafkaSink.<String>builder()
                        .setBootstrapServers(KAFKA_CONTAINER.getBootstrapServers())
                        .setRecordSerializer(
                                KafkaRecordSerializationSchema.builder()
                                        .setTopic(topic)
                                        .setValueSerializationSchema(serSchema)
                                        .setPartitioner(partitioner)
                                        .build())
                        .setDeliverGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                        .build());

        env.execute("Write sequence");
    }

    public static String getBootstrapServers() {
        return KAFKA_CONTAINER.getBootstrapServers();
    }

    public static void deleteTestTopic(String topic) {
        Map<String, Object> properties = new HashMap<>();
        properties.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, getBootstrapServers());
        try (AdminClient admin = AdminClient.create(properties)) {
            admin.deleteTopics(Collections.singletonList(topic));
        }
    }

    public Properties getStandardProps() {
        Properties standardProps = new Properties();
        standardProps.put("bootstrap.servers", KAFKA_CONTAINER.getBootstrapServers());
        standardProps.put("group.id", "flink-tests");
        standardProps.put("enable.auto.commit", false);
        standardProps.put("auto.offset.reset", "earliest");
        standardProps.put("max.partition.fetch.bytes", 256);
        standardProps.put("zookeeper.session.timeout.ms", zkTimeoutMills);
        standardProps.put("zookeeper.connection.timeout.ms", zkTimeoutMills);
        return standardProps;
    }

    public static List<String> readLines(String resource) throws IOException {
        final URL url = KafkaContainerTest.class.getClassLoader().getResource(resource);
        assert url != null;
        Path path = new File(url.getFile()).toPath();
        return Files.readAllLines(path);
    }

    private static void createTestTopic(String topic, int numPartitions, short replicationFactor)
            throws ExecutionException, InterruptedException, TimeoutException {
        final CreateTopicsResult result =
                admin.createTopics(
                        Collections.singletonList(
                                new NewTopic(topic, numPartitions, replicationFactor)));
        result.all().get(1, TimeUnit.MINUTES);
    }

    /**
     * Exposes information about how man records have been emitted overall and finishes after
     * receiving the checkpoint completed event.
     */
    private static final class InfiniteIntegerSource
            implements SourceFunction<Long>, CheckpointListener, CheckpointedFunction {

        private final SharedReference<AtomicLong> emittedRecordsCount;
        private final SharedReference<AtomicLong> emittedRecordsWithCheckpoint;

        private volatile boolean running = true;
        private volatile long temp;
        private Object lock;

        InfiniteIntegerSource(
                SharedReference<AtomicLong> emittedRecordsCount,
                SharedReference<AtomicLong> emittedRecordsWithCheckpoint) {
            this.emittedRecordsCount = emittedRecordsCount;
            this.emittedRecordsWithCheckpoint = emittedRecordsWithCheckpoint;
        }

        @Override
        public void run(SourceContext<Long> ctx) throws Exception {
            lock = ctx.getCheckpointLock();
            while (running) {
                synchronized (lock) {
                    ctx.collect(emittedRecordsCount.get().addAndGet(1));
                    Thread.sleep(1);
                }
            }
        }

        @Override
        public void cancel() {
            running = false;
        }

        @Override
        public void notifyCheckpointComplete(long checkpointId) throws Exception {
            emittedRecordsWithCheckpoint.get().set(temp);
            running = false;
            LOG.info("notifyCheckpointCompleted {}", checkpointId);
        }

        @Override
        public void snapshotState(FunctionSnapshotContext context) throws Exception {
            temp = emittedRecordsCount.get().get();
            LOG.info(
                    "snapshotState, {}, {}",
                    context.getCheckpointId(),
                    emittedRecordsCount.get().get());
        }

        @Override
        public void initializeState(FunctionInitializationContext context) throws Exception {
        }
    }
}