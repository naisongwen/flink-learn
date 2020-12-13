package org.learn.flink.connector.hive;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.table.api.*;
import org.apache.flink.table.api.bridge.java.BatchTableEnvironment;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.flink.table.catalog.hive.HiveCatalog;
import org.apache.flink.table.types.DataType;
import org.apache.hadoop.hive.conf.HiveConf;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;

import static org.apache.flink.table.api.config.ExecutionConfigOptions.TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM;

public class HiveTableJob {

    static final String TEST_CATALOG_NAME = "test-catalog-1";
    static final String HIVE_WAREHOUSE_URI_FORMAT = "jdbc:derby:;databaseName=%s;create=true";

    public static TableEnvironment createTableEnvWithBlinkPlannerBatchMode() {
        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inBatchMode().build();
        TableEnvironment tableEnv = TableEnvironment.create(settings);
        tableEnv.getConfig().getConfiguration().setInteger(TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM.key(), 1);
        return tableEnv;
    }

    static HiveConf createHiveConf() {
        ClassLoader classLoader = new HiveTableJob().getClass().getClassLoader();
        HiveConf.setHiveSiteLocation(classLoader.getResource("hive-site.xml"));

        try {
            TemporaryFolder TEMPORARY_FOLDER = new TemporaryFolder();
            TEMPORARY_FOLDER.create();
            String warehouseDir = TEMPORARY_FOLDER.newFolder().getAbsolutePath() + "/metastore_db";
            String warehouseUri = String.format(HIVE_WAREHOUSE_URI_FORMAT, warehouseDir);

            HiveConf hiveConf = new HiveConf();
            hiveConf.setVar(HiveConf.ConfVars.METASTOREWAREHOUSE, TEMPORARY_FOLDER.newFolder("hive_warehouse").getAbsolutePath());
            hiveConf.setVar(HiveConf.ConfVars.METASTORECONNECTURLKEY, warehouseUri);
            return hiveConf;
        } catch (IOException e) {
            throw new CatalogException(
                    "Failed to create test HiveConf to HiveCatalog.", e);
        }
    }

    static void createHiveTable(HiveCatalog hiveCatalog, String catalogName, String database) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        BatchTableEnvironment tableEnv = BatchTableEnvironment.create(env);
//        TableEnvironment tableEnv = createTableEnvWithBlinkPlannerBatchMode();
        tableEnv.registerCatalog(catalogName, hiveCatalog);
        tableEnv.useCatalog(catalogName);
        tableEnv.useDatabase(database);
        tableEnv.sqlUpdate("drop table IF EXISTS Orders");
        String sinkPath = new File("csv-order-sink").toURI().toString();
        tableEnv.sqlUpdate(String.format("create table Orders(id int,name string) with('connector.type' = 'filesystem','format.type'='csv','connector.path'='%s')",sinkPath));
//        tableEnv.sqlUpdate("insert into Orders select * from Products");
        tableEnv.sqlUpdate("insert into Orders values(1,'mac book'),(2,'iphone')");
        tableEnv.execute("create hive table");
    }

    static Table createQueryTable(HiveCatalog hiveCatalog,String catalogName,String database,String querySql) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
//        BatchTableEnvironment tableEnv = BatchTableEnvironment.create(env);
        TableEnvironment tableEnv = createTableEnvWithBlinkPlannerBatchMode();
        tableEnv.registerCatalog(catalogName, hiveCatalog);
        tableEnv.useCatalog(catalogName);
        tableEnv.useDatabase(database);
        Table src = tableEnv.sqlQuery(querySql);
//        TupleTypeInfo<Tuple2<Integer, String>> tupleType = new TupleTypeInfo<Tuple2<Integer, String>>(
//                BasicTypeInfo.INT_TYPE_INFO,
//                BasicTypeInfo.STRING_TYPE_INFO);
//        DataSet<Tuple2<Integer, String>> dsTuple =tableEnv.toDataSet(src, tupleType);
//        dsTuple.print();
        return src;
    }

    static RowTypeInfo writeToHiveTable(HiveCatalog hiveCatalog,Table srcTbl,String dbName, String tblName, TableSchema tableSchema, int numPartCols) throws Exception {
        RowTypeInfo rowTypeInfo=HiveUtils.createHiveTable(hiveCatalog,dbName, tblName,tableSchema,numPartCols);
        ObjectPath tablePath = new ObjectPath(dbName, tblName);
        srcTbl.insertInto(tablePath.getFullName());
        return rowTypeInfo;
    }

    public static void main(String[] args) throws Exception {
        String dbName = "default";
        String tblName = "dest";
        HiveCatalog hiveCatalog=HiveUtils.createCatalog(TEST_CATALOG_NAME,"default");
        createHiveTable(hiveCatalog,TEST_CATALOG_NAME,dbName);
        Table srcTable=createQueryTable(hiveCatalog,TEST_CATALOG_NAME,"default","select * from Orders");
        TableSchema.Builder builder = new TableSchema.Builder();
        builder.fields(new String[]{"id", "name"},
                new DataType[]{
                        DataTypes.INT(),
                        DataTypes.STRING()});
        TableSchema tableSchema=builder.build();
        ObjectPath tablePath = new ObjectPath(dbName, tblName);
        hiveCatalog.close();

        hiveCatalog=HiveUtils.createDefaultHiveCatalog();
        hiveCatalog.dropTable(tablePath,true);
        RowTypeInfo rowTypeInfo=writeToHiveTable(hiveCatalog,srcTable,dbName,tblName,tableSchema,0);
        Table distTable=createQueryTable(hiveCatalog,"hive","default","select * from dest");
        hiveCatalog.close();
    }
}