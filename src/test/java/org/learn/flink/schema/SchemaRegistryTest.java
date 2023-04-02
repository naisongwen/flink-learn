package org.learn.flink.schema;

import org.apache.avro.specific.SpecificRecordBase;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.formats.avro.registry.confluent.ConfluentRegistryAvroDeserializationSchema;
import org.apache.flink.formats.avro.registry.confluent.ConfluentRegistryAvroSerializationSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.CollectionUtil;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.Before;
import org.junit.Test;
import org.learn.flink.connector.kafka.KafkaConf;

import java.io.Serializable;
import java.util.List;
import java.util.Properties;

import static org.learn.flink.connector.kafka.KafkaConf.schemaRegistryUrl;

public class SchemaRegistryTest {
    String kafkaServer = KafkaConf.servers;
    //    String kafkaServer = "http://10.201.0.89:9101";

    String writeSql = "insert into user_created values(1,'aaaa',22),(1,'bbb',23)";
    String querySql = "select * from user_created";
    TableEnvironment tEnv;

    @Before
    public void init(){
        EnvironmentSettings settings = EnvironmentSettings.newInstance().inBatchMode().build();
        tEnv = TableEnvironment.create(settings);
    }

    /***
     * https://hevodata.com/learn/kafka-schema-registry/#u
     * docker run -p 8081:8081 -e  SCHEMA_REGISTRY_KAFKASTORE_CONNECTION_URL=10.201.0.82:9092  -e SCHEMA_REGISTRY_HOST_NAME=localhost  -e SCHEMA_REGISTRY_LISTENERS=http://0.0.0.0:8081  -e SCHEMA_REGISTRY_DEBUG=true confluentinc/cp-schema-registry
     *
     * https://docs.confluent.io/platform/current/schema-registry/schema_registry_onprem_tutorial.html#run-the-producer
     *
     */
    @Test
    public void testRegistry() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        TableEnvironment tEnv = StreamTableEnvironment.create(env);

        //https://nightlies.apache.org/flink/flink-docs-release-1.14/docs/connectors/table/formats/avro-confluent/
        String createTableSql = String.format("CREATE TABLE user_created (\n" +
                "\n" +
                "  -- one column mapped to the Kafka raw UTF-8 key\n" +
                "  the_kafka_key STRING,\n" +
                "  \n" +
                "  -- a few columns mapped to the Avro fields of the Kafka value\n" +
                "  id STRING,\n" +
                "  name STRING, \n" +
                "  email STRING\n" +
                "\n" +
                ") WITH (\n" +
                "  'connector' = 'kafka',\n" +
                "  'topic' = 'user_events_example1',\n" +
                "  'scan.startup.mode'='earliest-offset',\n" +
                "  'properties.group.id' = 'kafka registry',\n" +
                "  'properties.bootstrap.servers' = '%s',\n" +
                "  -- UTF-8 string as Kafka keys, using the 'the_kafka_key' table column\n" +
                "  'key.format' = 'raw',\n" +
                "  'key.fields' = 'the_kafka_key',\n" +
                "  'value.format' = 'avro-confluent',\n" +
                "  'value.avro-confluent.schema-registry.url' = '%s',\n" +
                "  'value.fields-include' = 'EXCEPT_KEY'\n" +
                "  -- subjects have a default value since Flink 1.13, though can be overridden:\n" +
//                "  -- 'key.avro-confluent.subject' = 'user_events_example2-key2',\n" +
//                "  -- 'value.avro-confluent.subject' = 'user_events_example2-value2'"
                ")", kafkaServer, schemaRegistryUrl);

        tEnv.executeSql(createTableSql);

        String insertSql = "insert into user_created values('kk','1','wns','wennaisong@deepexi.com')";
        tEnv.executeSql(insertSql);

        TableResult result = tEnv.executeSql(querySql);
        List<Row> res = CollectionUtil.iteratorToList(result.collect());
        res.forEach(System.out::println);
    }

    @Test
    public void testReadKafka() throws Exception {

        String createTableSql = String.format("CREATE TABLE user_created (\n" +
                "content string" +
                ") WITH (\n" +
                "  'connector' = 'kafka',\n" +
                "  'topic' = 'user_events_example1',\n" +
                "  'scan.startup.mode'='earliest-offset',\n" +
                "  'properties.key.serializer'='" + StringSerializer.class.getName() + "',\n" +
                "  'properties.value.serializer'='" + StringSerializer.class.getName() + "',\n" +
                "  'properties.key.deserializer'='" + StringDeserializer.class.getName() + "',\n" +
                "  'properties.value.deserializer'='" + StringDeserializer.class.getName() + "',\n" +
                "  'value.format'='raw',\n" +
                "  'value.format' = 'avro-confluent',\n" +
                "  'properties.group.id' = 'kafka registry',\n" +
                "  'properties.bootstrap.servers' = '%s'\n" +
                ")", kafkaServer, schemaRegistryUrl);

        tEnv.executeSql(createTableSql);
        TableResult result = tEnv.executeSql(querySql);
        List<Row> res = CollectionUtil.iteratorToList(result.collect());
        res.forEach(System.out::println);
    }

    @Test
    public void testStreamWriteKafka() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        TableEnvironment tEnv = StreamTableEnvironment.create(env);

        String createTableSql = String.format("CREATE TABLE user_created (\n" +
                "      `id` INT," +
                "      `name` String," +
                "      `age` INT" +
                ") WITH (\n" +
                "  'connector' = 'kafka',\n" +
                "  'topic' = 'yjh_test',\n" +
                "  'properties.bootstrap.servers' = '%s',\n" +
                "  'properties.commit.offsets.on.checkpoint' = 'true',\n" +
                "  'properties.group.id' = 'test1',\n" +
                "  'scan.startup.mode' = 'earliest-offset',\n" +
                "  'value.format' = 'avro-confluent',\n" +
                "  'value.avro-confluent.subject' = 'yjh_me',\n" +
//                        "   'key.format' = 'raw'," +
//                        "   'key.fields' = 'id'," +
                "   'value.avro-confluent.url' = '%s',\n" +
                "  'properties.key.deserializer' = 'org.apache.kafka.common.serialization.StringDeserializer',\n" +
                "  'properties.value.deserializer' = 'io.confluent.kafka.serializers.KafkaAvroDeserializer',\n" +
                "  'value.fields-include' = 'ALL'\n" +
                ")", kafkaServer, schemaRegistryUrl);

        tEnv.executeSql(createTableSql);
        tEnv.executeSql(writeSql).print();
//        env.execute();
    }


    @Test
    public void testStreamReadKafka() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        String createTableSql = String.format("CREATE TABLE user_created (\n" +
                "      `id` INT," +
                "      `name` String," +
                "      `age` INT" +
                ") WITH (\n" +
                "  'connector' = 'kafka',\n" +
                "  'topic' = 'yjh_test',\n" +
                "  'properties.bootstrap.servers' = '%s',\n" +
                "  'properties.commit.offsets.on.checkpoint' = 'true',\n" +
                "  'properties.group.id' = 'test1',\n" +
                "  'scan.startup.mode' = 'earliest-offset',\n" +
                "  'value.format' = 'avro-confluent',\n" +
                "  'value.avro-confluent.subject' = 'yjh_me',\n" +
//                        "   'key.format' = 'raw'," +
//                        "   'key.fields' = 'id'," +
                "   'value.avro-confluent.url' = '%s',\n" +
                "  'properties.key.deserializer' = 'org.apache.kafka.common.serialization.StringDeserializer',\n" +
                "  'properties.value.deserializer' = 'io.confluent.kafka.serializers.KafkaAvroDeserializer',\n" +
                "  'value.fields-include' = 'ALL'\n" +
                ")", kafkaServer, schemaRegistryUrl);

        tEnv.executeSql(createTableSql);
        Table table = tEnv.sqlQuery(querySql);
        tEnv.toAppendStream(table, Row.class).print();
//
//        TableResult result = tEnv.sqlQuery(querySql).execute();
//        List<Row> res = CollectionUtil.iteratorToList(result.collect());
//        res.forEach(System.out::println);
        env.execute();
    }
}
