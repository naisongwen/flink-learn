package org.learn.flink.connector.jdbc;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.jdbc.JDBCInputFormat;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.Row;
import org.learn.flink.connector.jdbc.meta.MysqlDbMetadata;

import static org.apache.flink.table.types.utils.TypeConversions.fromLegacyInfoToDataType;

public class JdbcSourceJob {
    public static void main(String[] args) throws Exception {
        MysqlDbMetadata mysqlDbMetadata = new MysqlDbMetadata("airflow");
        ExecutionEnvironment environment = ExecutionEnvironment.getExecutionEnvironment();

        TypeInformation<?>[] fieldTypes = new TypeInformation<?>[]{
                BasicTypeInfo.INT_TYPE_INFO,
                BasicTypeInfo.STRING_TYPE_INFO,
                BasicTypeInfo.STRING_TYPE_INFO,
                BasicTypeInfo.STRING_TYPE_INFO,
                BasicTypeInfo.BOOLEAN_TYPE_INFO
        };

        RowTypeInfo rowTypeInfo = new RowTypeInfo(fieldTypes);
        RowType rowType = (RowType) fromLegacyInfoToDataType(rowTypeInfo).getLogicalType();

        JDBCInputFormat.JDBCInputFormatBuilder inputBuilder = JDBCInputFormat.buildJDBCInputFormat()
                .setDrivername(mysqlDbMetadata.getDriverClass())
                .setDBUrl(mysqlDbMetadata.getUrl())
                .setQuery("select * from airflow.users limit 10")
                .setUsername(mysqlDbMetadata.getLoginAccount())
                .setPassword(mysqlDbMetadata.getLoginPwd())
                .setRowTypeInfo(rowTypeInfo);
                //.setRowConverter(JDBCDialects.get(mysqlDbMetadata.getUrl()).get().getRowConverter(rowType));

        final int fetchSize = 1;
        //use a "splittable" query to exploit parallelism
//        inputBuilder = inputBuilder
//                .setParametersProvider(new NumericBetweenParametersProvider(1, 10).ofBatchSize(fetchSize));

        DataSet<Row> source = environment.createInput(inputBuilder.finish());
        source.print();
    }
}
