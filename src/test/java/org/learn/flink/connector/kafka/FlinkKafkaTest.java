package org.learn.flink.connector.kafka;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.hive.HiveCatalog;
import org.apache.flink.types.Row;
import org.junit.Test;
import org.learn.flink.connector.hive.HiveUtils;

import java.util.ArrayList;
import java.util.List;

//KafkaTableITCase
public class FlinkKafkaTest {

    @Test
    public void testSinkKafka() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
        env.setParallelism(1);
        env.getConfig().setRestartStrategy(RestartStrategies.noRestart());
        EnvironmentSettings environmentSettings = EnvironmentSettings.newInstance().inStreamingMode().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, environmentSettings);
        tableEnv.registerCatalog("hive", HiveUtils.createCatalog("hive","default"));
        tableEnv.useCatalog("hive");
        String sql="CREATE TABLE if not exists KafkaTable1 (\n" +
                "  `user_id` BIGINT,\n" +
                "  `item_id` BIGINT,\n" +
                "  `behavior` STRING\n" +
                ") WITH (\n" +
                "  'connector' = 'kafka',\n" +
                "  'topic' = 'user_behavior',\n" +
                "  'is_generic' = 'false',\n" +
                String.format("  'properties.bootstrap.servers' = '%s',\n",KafkaConf.servers) +
                "  'properties.group.id' = 'testGroup',\n" +
                "  'scan.startup.mode' = 'earliest-offset',\n" +
                "  'format' = 'csv'\n" +
                ")";
        tableEnv.executeSql(sql);
        tableEnv.executeSql("insert into KafkaTable (`user_id`,`item_id`,`behavior`) values(100,101,'look'),(1000,1001,'ordered'),(10000,10001,'cancel')").await();
    }

    @Test
    public void testReadKafka() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
        env.setParallelism(1);
        env.getConfig().setRestartStrategy(RestartStrategies.noRestart());
        EnvironmentSettings environmentSettings = EnvironmentSettings.newInstance().inStreamingMode()
                .build();
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(env, environmentSettings);
        String sql="CREATE TABLE KafkaTable (\n" +
                "  `user_id` BIGINT,\n" +
                "  `item_id` BIGINT,\n" +
                "  `behavior` STRING,\n" +
                "  `ts` TIMESTAMP(3) METADATA FROM 'timestamp'\n" +
                ") WITH (\n" +
                "  'connector' = 'kafka',\n" +
                "  'topic' = 'user_behavior',\n" +
                String.format("  'properties.bootstrap.servers' = '%s',\n",KafkaConf.servers) +
                "  'properties.group.id' = 'testGroup',\n" +
                "  'scan.startup.mode' = 'earliest-offset',\n" +
                "  'format' = 'csv'\n" +
                ")";
        tableEnvironment.executeSql(sql);
        Table src=tableEnvironment.sqlQuery("select * from KafkaTable");
        DataStream<Row> result = tableEnvironment.toDataStream(src);
        TestingSinkFunction sink = new TestingSinkFunction();
        result.addSink(sink).setParallelism(1);
        env.execute("SQL Kafka Job");
    }

    private static final class TestingSinkFunction implements SinkFunction<Row> {

        private static final long serialVersionUID = 455430015321124493L;
        private static final List<String> rows = new ArrayList<>();

        private TestingSinkFunction() {
            rows.clear();
        }

        @Override
        public void invoke(Row value, Context context) {
            rows.add(value.toString());
            System.out.println(value);
        }
    }
}
