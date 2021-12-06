package org.learn.flink.connector.hive;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.catalog.hive.HiveCatalog;

import java.io.File;

public class SqlHiveTable {
    public static void main(String[] args) throws Exception {
        String catalogName="hive";
        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        TableEnvironment tableEnv = TableEnvironment.create(settings);
        tableEnv.getConfig().getConfiguration().setInteger(ExecutionConfigOptions.TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM, 1);
        HiveCatalog hiveCatalog=HiveUtils.createCatalog(catalogName,"default");
        tableEnv.registerCatalog(catalogName, hiveCatalog);
        tableEnv.useCatalog(catalogName);

        String srcPath = new SqlHiveTable().getClass().getResource("/csv/test3.csv").getPath();

        tableEnv.sqlUpdate("drop table if exists src");
        tableEnv.sqlUpdate("drop table if exists  sink");
        tableEnv.executeSql("CREATE TABLE src (" +
                "price DECIMAL(10, 2),currency STRING,ts6 TIMESTAMP(6),ts AS CAST(ts6 AS TIMESTAMP(3)),WATERMARK FOR ts AS ts) " +
                String.format("WITH ('connector.type' = 'filesystem','connector.path' = 'file://%s','format.type' = 'csv')", srcPath));

        String sinkPath = new File("csv-order-sink").toURI().toString();

        tableEnv.sqlUpdate("CREATE TABLE sink (" +
                "window_end TIMESTAMP(3),max_ts TIMESTAMP(6),counter BIGINT,total_price DECIMAL(10, 2)) " +
                String.format("WITH ('connector.type' = 'filesystem','connector.path' = '%s','format.type' = 'csv')", sinkPath));

        tableEnv.sqlUpdate("INSERT into sink " +
                "SELECT TUMBLE_END(ts, INTERVAL '5' SECOND),MAX(ts6),COUNT(*),MAX(price) FROM src " +
                "GROUP BY TUMBLE(ts, INTERVAL '5' SECOND)");

        tableEnv.execute("testJob");
    }
}
