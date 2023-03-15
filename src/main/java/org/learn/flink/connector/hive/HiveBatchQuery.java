package org.learn.flink.connector.hive;

import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.table.api.*;
import org.apache.flink.table.catalog.hive.HiveCatalog;
import org.apache.flink.types.Row;
import org.apache.flink.util.CollectionUtil;
import org.apache.hive.common.util.HiveVersionInfo;

import java.nio.charset.StandardCharsets;
import java.util.List;

public class HiveBatchQuery {
    public static void main(String[] args) {
        EnvironmentSettings settings = EnvironmentSettings.inBatchMode();
        TableEnvironment tEnv = TableEnvironment.create(settings);

        tEnv.getConfig().addConfiguration(GlobalConfiguration.loadConfiguration());
        String database = "tpcds_bin_orc_1000";
        String hiveConf="src/main/resources/";
        String catalogName="myHiveCatalog";
        HiveCatalog catalog = new HiveCatalog(catalogName, database, hiveConf,HiveVersionInfo.getVersion());
        tEnv.registerCatalog(catalogName, catalog);
        tEnv.useCatalog(catalogName);
        tEnv.createTable("myTable", TableDescriptor.forConnector("datagen")
                        .schema(Schema.newBuilder()
                                .column("f0", DataTypes.STRING())
                                .build()).build());
//        TableResult table=tEnv.executeSql("show databases");
//        List<Row> res = CollectionUtil.iteratorToList(table.collect());
        ClassLoader cl = HiveBatchQuery.class.getClassLoader();
        InputStream inputStream=cl.getResourceAsStream("queries/q63.sql");
        String sql=streamToString(inputStream);
        Table table=tEnv.sqlQuery(sql);
        String plan=table.explain();
        System.out.println(plan);
        List<Row> res =CollectionUtil.iteratorToList(table.execute().collect());
        System.out.println(res);
    }

    static String streamToString(InputStream inputStream) {
        BufferedInputStream in = new BufferedInputStream(inputStream);
        ByteArrayOutputStream outStream = new ByteArrayOutputStream();
        try {
            int c;
            while ((c = in.read()) != -1) {
                outStream.write(c);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            try {
                in.close();
            } catch (IOException ignored) {
            }
        }
        return new String(outStream.toByteArray(), StandardCharsets.UTF_8);
    }
}