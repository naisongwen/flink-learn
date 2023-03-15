package org.learn.flink.connector.filesystem;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class S3SinkIcebergExample {

    public static void main(String[] args) throws Exception {
        Configuration globalConfiguration = GlobalConfiguration.loadConfiguration();
        globalConfiguration.set(CoreOptions.ALLOWED_FALLBACK_FILESYSTEMS, "s3a");
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(globalConfiguration);
        env.setParallelism(1);
        env.getConfig().setRestartStrategy(RestartStrategies.noRestart());
        EnvironmentSettings environmentSettings = EnvironmentSettings.newInstance().inStreamingMode()
                .useBlinkPlanner()
                .build();
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(env, environmentSettings);

        String s3tableSql = "create table store_sales(\n" +
                "      ss_sold_date_sk bigint\n" +
                ",     ss_sold_time_sk bigint\n" +
                ",     ss_item_sk bigint\n" +
                ",     ss_customer_sk bigint\n" +
                ",     ss_cdemo_sk bigint\n" +
                ",     ss_hdemo_sk bigint\n" +
                ",     ss_addr_sk bigint\n" +
                ",     ss_store_sk bigint\n" +
                ",     ss_promo_sk bigint\n" +
                ",     ss_ticket_number bigint\n" +
                ",     ss_quantity int\n" +
                ",     ss_wholesale_cost decimal(7,2)\n" +
                ",     ss_list_price decimal(7,2)\n" +
                ",     ss_sales_price decimal(7,2)\n" +
                ",     ss_ext_discount_amt decimal(7,2)\n" +
                ",     ss_ext_sales_price decimal(7,2)\n" +
                ",     ss_ext_wholesale_cost decimal(7,2)\n" +
                ",     ss_ext_list_price decimal(7,2)\n" +
                ",     ss_ext_tax decimal(7,2)\n" +
                ",     ss_coupon_amt decimal(7,2)\n" +
                ",     ss_net_paid decimal(7,2)\n" +
                ",     ss_net_paid_inc_tax decimal(7,2)\n" +
                ",     ss_net_profit decimal(7,2)\n" +
                ")  partitioned by (ss_sold_date_sk) " +
                "WITH (" +
                "'format' = 'csv'," +
                "'connector' = 'filesystem'," +
                "'path' = 's3a://dev-tarim/tpcds-data/1TB/web_sales/web_sales_1_5.dat')";
        tableEnvironment.executeSql(s3tableSql);
        tableEnvironment.executeSql("select * from default_catalog.default_database.store_sales limit 10").print();
    }
}