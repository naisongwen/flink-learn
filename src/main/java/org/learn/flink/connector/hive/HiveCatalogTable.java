package org.learn.flink.connector.hive;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.apache.flink.util.CollectionUtil;

import java.io.File;
import java.util.List;

public class HiveCatalogTable {
    public static void main(String[] args) throws Exception {
        String catalogName = "hive";
        EnvironmentSettings settings = EnvironmentSettings.newInstance().inBatchMode().build();
        TableEnvironment tableEnv = TableEnvironment.create(settings);
        tableEnv.getConfig().getConfiguration().setInteger(ExecutionConfigOptions.TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM, 1);
//        HiveCatalog hiveCatalog = HiveUtils.createCatalog(catalogName, "default");
//        tableEnv.registerCatalog(catalogName, hiveCatalog);
//        tableEnv.useCatalog(catalogName);
        tableEnv.executeSql("CREATE CATALOG hive_catalog WITH (\n" +
                "    'type'='hive',\n" +
//                "    'uri'='thrift://10.0.30.12:9083',\n" +
//                "    'hive-version'='2.3.3'\n" +
                String.format("'hive-conf-dir'='%s'", new File("src/main/resources/").getAbsolutePath()) +
                ")");
        String table = "test_table_4";
        tableEnv.executeSql("use catalog hive_catalog");
        tableEnv.executeSql(String.format("CREATE TABLE %s (\n" +
                "  CD_ID BIGINT,\n" +
                "  PRIMARY KEY (CD_ID) NOT ENFORCED\n" +
                ") WITH (\n" +
                "   'connector' = 'jdbc',\n" +
                "   'username' = 'hive',\n" +
                "   'password' = 'hive',\n" +
                "   'url' = 'jdbc:mysql://10.0.30.12:3306/hive',\n" +
                "   'table-name' = 'CDS'\n" +
                ")", table));

        CloseableIterator<Row> iterator = tableEnv.sqlQuery(String.format("select * from %s", table)).execute().collect();
        List<Row> res = CollectionUtil.iteratorToList(iterator);
        System.out.println(res);
    }
}
