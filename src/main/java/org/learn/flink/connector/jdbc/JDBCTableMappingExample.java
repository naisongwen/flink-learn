package org.learn.flink.connector.jdbc;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.CatalogDatabaseImpl;
import org.apache.flink.table.catalog.hive.HiveCatalog;
import org.apache.flink.table.catalog.hive.client.HiveShimLoader;
import org.apache.hadoop.hive.conf.HiveConf;

import java.util.HashMap;
import java.util.Map;


public class JDBCTableMappingExample {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
        env.setParallelism(1);
        EnvironmentSettings environmentSettings = EnvironmentSettings.newInstance().inStreamingMode()
                .build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, environmentSettings);
        HiveConf hiveConf = new HiveConf();
        String hmsUri = "thrift://10.201.0.212:49156";
        hiveConf.set("hive.metastore.uris", hmsUri);
        hiveConf.set("hive.metastore.warehouse.dir", "s3a://faas-ethan/");
        hiveConf.set("metastore.catalog.default", "hive");
        hiveConf.set("hive.metastore.client.capability.check", "false");
        hiveConf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");
        hiveConf.set("fs.s3a.access.key", "admin1234");
        hiveConf.set("fs.s3a.connection.ssl.enabled", "false");
        hiveConf.set("fs.s3a.secret.key", "admin1234");
        hiveConf.set("fs.s3a.endpoint", "http://10.201.0.212:32000");

        Map<String, String> properties = new HashMap<>();
        properties.put("test", "test");
        HiveCatalog hiveCatalog = new HiveCatalog("test_catalog_name", "default", hiveConf, HiveShimLoader.getHiveVersion());
        hiveCatalog.open();

        tableEnv.registerCatalog(hiveCatalog.getName(), hiveCatalog);
        tableEnv.useCatalog(hiveCatalog.getName());

//        CatalogDatabaseImpl catalogDatabase = new CatalogDatabaseImpl(properties, "test");
//        hiveCatalog.createDatabase("test_database", catalogDatabase, true);
//        tableEnv.useDatabase("test_database");


        String tblName = "test_iceberg_table_16";
        tableEnv.executeSql(String.format("drop table if exists %s", tblName));
        String sqlQuery = String.format("CREATE TABLE %s (" +
                "    param_key  varchar(255),\n" +
                "    param_value varchar(255)\n" +
                ") WITH (\n" +
                "   'connector' = 'jdbc',\n" +
                "   'username'='root',\n" +
                "   'password'='123456',\n" +
                "   'url' = 'jdbc:mysql://10.201.0.205:3306/hivemetastore_db_dev_212',\n" +
                "   'table-name' = 'table_params'\n" +
                ")", tblName);
        tableEnv.executeSql(sqlQuery);
    }
}
