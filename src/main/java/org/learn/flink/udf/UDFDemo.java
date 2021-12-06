package org.learn.flink.udf;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.FunctionParameter;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.functions.FunctionKind;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.functions.UserDefinedFunction;
import org.apache.flink.table.sinks.RetractStreamTableSink;
import org.apache.flink.table.types.extraction.TypeInferenceExtractor;
import org.apache.flink.table.types.inference.TypeInference;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;

import java.util.List;

public class UDFDemo {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment streamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings blinkStreamSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(streamExecutionEnvironment, blinkStreamSettings);
        tableEnvironment.createTemporarySystemFunction("MaxValue", new MaxValueScalarFunction());
        Table table = tableEnvironment.sqlQuery("SELECT MaxValue(1,2,3)");
        //Table table = tableEnvironment.sqlQuery("SELECT MaxValue(1.2, 2.2, -1, -2.3)");
        CloseableIterator<Row> closable=table.execute().collect();
        Row row=closable.next();
        System.out.print(row);
//        DataStream<Row> appendStreamTableResult = tableEnvironment.toAppendStream(table, Row.class);
//        appendStreamTableResult.print();
        //Table table = tableEnvironment.sqlQuery("SELECT MaxValue(1.2, 2.2, -1d, -2.3)");
    }
}
