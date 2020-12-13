package org.learn.flink.connector.kafka;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.util.ArrayList;
import java.util.List;

public class SqlKafkaJob {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(
                env,
                EnvironmentSettings.newInstance()
                        // watermark is only supported in blink planner
                        .useBlinkPlanner()
                        .inStreamingMode()
                        .build()
        );
        env.getConfig().setRestartStrategy(RestartStrategies.noRestart());
        env.setParallelism(1);
        String ddmSql = "CREATE TABLE random_number (\n" +
                "    id BIGINT\n" +
                ") WITH (\n" +
                "    'connector.type' = 'kafka', -- 使用 kafka connector\n" +
                "    'connector.version' = '0.10',  -- kafka 版本，universal 支持 0.11 以上的版本\n" +
                "    'connector.topic' = 'sql-flink-topic-test',  -- kafka topic\n" +
                "    'update-mode' = 'append',\n" +
                "    'connector.startup-mode' = 'earliest-offset', -- 从起始 offset 开始读取\n" +
                "    'connector.properties.0.key' = 'security.protocol',\n" +
                "    'connector.properties.0.value' = 'SASL_PLAINTEXT', \n" +
                "    'connector.properties.1.key' = 'sasl.mechanism',\n" +
                "    'connector.properties.1.value' = 'PLAIN', \n" +
                "    'connector.properties.2.key' = 'sasl.jaas.config',\n" +
                "    'connector.properties.2.value' = 'org.apache.kafka.common.security.plain.PlainLoginModule required username=\"zkadmin\" password=\"zkadmin-pwd\";', \n" +
                "    'connector.properties.3.key' = 'bootstrap.servers',\n" +
                "    'connector.properties.3.value' = '47.110.20.43:2093', \n" +
                "    'connector.properties.4.key' = 'group.id', \n" +
                "    'connector.properties.4.value' = 'sql kafka flink client', \n" +
                "    'format.type' = 'json',  -- 数据源格式为 json\n" +
                "    'format.derive-schema' = 'true' -- 从 DDL schema 确定 json 解析规则\n" +
                ")";
        tEnv.sqlUpdate(ddmSql);
        tEnv.sqlUpdate("insert into random_number values(100),(1000),(10000)");
        Table src=tEnv.sqlQuery("select id from random_number");
        tEnv.execute("sql");
        DataStream<Row> result = tEnv.toAppendStream(src, Row.class);
        result.print();
        TestingSinkFunction sink = new TestingSinkFunction(2);
        result.addSink(sink).setParallelism(1);
        // 提交作业
        env.execute("SQL Kafka Job");
    }

    private static final class TestingSinkFunction implements SinkFunction<Row> {

        private static final long serialVersionUID = 455430015321124493L;
        private static List<String> rows = new ArrayList<>();

        private final int expectedSize;

        private TestingSinkFunction(int expectedSize) {
            this.expectedSize = expectedSize;
            rows.clear();
        }

        @Override
        public void invoke(Row value, Context context) {
            rows.add(value.toString());
        }
    }
}
