package org.learn.flink.connector.hive;

import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.CatalogTableImpl;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.hive.HiveCatalog;
import org.apache.flink.table.catalog.hive.client.HiveShimLoader;

import java.util.Arrays;
import java.util.HashMap;

public class HiveUtils {


    public static HiveCatalog createDefaultHiveCatalog() {
        //HiveConf hiveConf=createHiveConf();
        HiveCatalog hiveCatalog=new HiveCatalog("hive", null, "src/main/resources/", HiveShimLoader.getHiveVersion());
        hiveCatalog.open();
        return  hiveCatalog;
    }

    public static HiveCatalog createCatalog(String catalogName,String databaseName) {
        return new HiveCatalog(catalogName, databaseName, "src/main/resources/", HiveShimLoader.getHiveVersion());
    }

    public static RowTypeInfo createHiveTable(HiveCatalog hiveCatalog,String dbName, String tblName, TableSchema tableSchema, int numPartCols) throws Exception {
        CatalogTable catalogTable=null;//= createHiveCatalogTable(tableSchema, numPartCols);
        hiveCatalog.createTable(new ObjectPath(dbName, tblName), catalogTable, false);
        return new RowTypeInfo(tableSchema.getFieldTypes(), tableSchema.getFieldNames());
    }
}
