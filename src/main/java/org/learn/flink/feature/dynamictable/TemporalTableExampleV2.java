package org.learn.flink.feature.dynamictable;

import com.google.common.collect.Lists;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.util.List;

import static org.apache.flink.table.api.Expressions.$;

public class TemporalTableExampleV2 {
    public static void main(String[] args) {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        env.setParallelism(1);

        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        List<Tuple2<Long, String>> orderData = Lists.newArrayList();
        orderData.add(new Tuple2<>(2L, "Euro"));
        orderData.add(new Tuple2<>(1L, "US Dollar"));
        orderData.add(new Tuple2<>(50L, "Yen"));
        orderData.add(new Tuple2<>(3L, "Euro"));
        orderData.add(new Tuple2<>(5L, "US Dollar"));

        List<Tuple2<String, Long>> rateHistoryData = Lists.newArrayList();
        rateHistoryData.add(new Tuple2<>("US Dollar", 102L));
        rateHistoryData.add(new Tuple2<>("Euro", 114L));
        rateHistoryData.add(new Tuple2<>("Yen", 1L));
        rateHistoryData.add(new Tuple2<>("Euro", 116L));
        rateHistoryData.add(new Tuple2<>("Euro", 119L));

        DataStream<Tuple2<Long, String>> orderStream = env.fromCollection(orderData);

        //https://nightlies.apache.org/flink/flink-docs-release-1.14/zh/docs/dev/table/concepts/time_attributes/#%E5%9C%A8-datastream-%E5%88%B0-table-%E8%BD%AC%E6%8D%A2%E6%97%B6%E5%AE%9A%E4%B9%89
        Table orderTable = tEnv.fromDataStream(orderStream,
                $("amount"), $("currency"), $("proctime").proctime());
        tEnv.registerTable("Orders", orderTable);
        orderTable.printSchema();//only for debug

        Table rateTable = tEnv.fromDataStream(env.fromCollection(rateHistoryData),
                $("currency"), $("rate"), $("proctime").proctime());
        tEnv.registerTable("RatesHistory", rateTable);
        rateTable.printSchema();//only for debug
        tEnv.registerFunction(
                "Rates",
                rateTable.createTemporalTableFunction("proctime", "currency"));

        String sqlQuery =
                "SELECT o.currency, o.amount, r.rate, " +
                        " o.amount * r.rate as amount_sum " +
                        "from " +
                        " Orders AS o, " +
                        " LATERAL TABLE (Rates(o.proctime)) AS r " +
                        "WHERE r.currency = o.currency";
        tEnv.sqlQuery(sqlQuery).execute().print();
    }
}