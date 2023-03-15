package org.learn.flink.connector.hive;

import com.google.common.collect.Lists;
import org.apache.flink.table.api.*;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.catalog.*;
import org.apache.flink.table.catalog.exceptions.DatabaseNotExistException;
import org.apache.flink.table.catalog.exceptions.TableAlreadyExistException;
import org.apache.flink.table.catalog.hive.HiveCatalog;
import org.apache.flink.table.catalog.hive.client.HiveShimLoader;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.apache.flink.util.CollectionUtil;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hive.common.util.HiveVersionInfo;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TestFlinkHiveSQL {
    String hmsUri = "thrift://10.201.0.212:39083";
    String defaultCatalog="aaaa_mapping_1982";
    String defaultDatabase="dlink_default";
    String table = "test_flink_hive_table_4";

    HiveConf hiveConf = new HiveConf();
    TableEnvironment tableEnv;
    HiveCatalog hiveCatalog;

    @Before
    public void init(){
        hiveConf.set("hive.metastore.uris", hmsUri);
        String warehouse= new File("warehouse").getAbsolutePath();
//        hiveConf.set("hive.metastore.warehouse.dir",warehouse);
        hiveConf.set("metastore.catalog.default", defaultCatalog);
        hiveConf.set("hive.metastore.client.capability.check", "false");

        EnvironmentSettings settings = EnvironmentSettings.newInstance().inBatchMode().build();
        tableEnv = TableEnvironment.create(settings);
        tableEnv.getConfig().getConfiguration().setInteger(ExecutionConfigOptions.TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM, 1);


        //tableEnv.getConfig().setSqlDialect(SqlDialect.HIVE);
        hiveCatalog = new HiveCatalog(defaultCatalog, defaultDatabase, hiveConf, HiveVersionInfo.getVersion());

        hiveCatalog.open();

        tableEnv.registerCatalog(hiveCatalog.getName(), hiveCatalog);
        tableEnv.useCatalog(hiveCatalog.getName());

    }

    private void showResult(TableResult tableResult){
        CloseableIterator<Row> iterator=tableResult.collect();
        List<Row> res = CollectionUtil.iteratorToList(iterator);
        System.out.println(res);
    }

    @Test
    public void testCreateTable() throws TableAlreadyExistException, DatabaseNotExistException {
        ObjectPath tablePath=new ObjectPath("test_db",table);
        //CatalogBaseTable table= CatalogTable.fromProperties();
        TableSchema tableSchema=TableSchema.builder().field("id",DataTypes.INT()).field("data",DataTypes.STRING()).build();
        CatalogTable catalogTable=new CatalogTableImpl(tableSchema, new HashMap<>(),"test crreate flink hive table");
        hiveCatalog.createTable(tablePath,catalogTable,false);
        tableEnv.createTable("myTable", TableDescriptor.forConnector("hive")
                .schema(Schema.newBuilder()
                        .column("f0", DataTypes.STRING())
                        .build()).build());
    }

    @Test
    public void main() {
        showResult(tableEnv.executeSql("show databases"));
//        tableEnv.executeSql("create DATABASE if not exists test_db LOCATION '/tmp/test_db'");
        tableEnv.executeSql("create DATABASE if not exists test_db with('hive.database.location-uri'='/tmp/test_db')");
//        tableEnv.executeSql(String.format("drop table if exists %s", table));
//        tableEnv.executeSql(String.format("CREATE TABLE %s (ID BIGINT)  with (" +
//                "'connector'='hive'," +
//                "'is_generic' = 'false'" +
//                ")",
//                table));
        tableEnv.executeSql(String.format("CREATE TABLE %s (ID BIGINT)", table));
        tableEnv.executeSql(String.format("insert into %s values (1)", table));
        tableEnv.executeSql(String.format("select * from %s", table));

        TableResult result=tableEnv.sqlQuery(String.format("select * from %s", table)).execute();
        showResult(result);
    }

    @After
    public void destroy(){
        //        tableEnv.executeSql(String.format("drop table %s", table));
        hiveCatalog.close();
    }
}

