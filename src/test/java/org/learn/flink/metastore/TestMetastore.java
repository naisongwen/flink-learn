package org.learn.flink.metastore;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.catalog.hive.HiveCatalog;
import org.junit.Test;
import org.learn.flink.connector.hive.HiveUtils;

import java.net.InetAddress;
import java.net.UnknownHostException;

public class TestMetastore {
    @Test
    public void test() throws UnknownHostException {
        InetAddress address= InetAddress.getByName("10.201.0.214");
        String catalogName = "hive";
        EnvironmentSettings settings = EnvironmentSettings.newInstance().inBatchMode().build();
        TableEnvironment tableEnv = TableEnvironment.create(settings);
        HiveCatalog hiveCatalog = HiveUtils.createCatalog(catalogName, "default");
        tableEnv.registerCatalog(catalogName, hiveCatalog);
    }
}
