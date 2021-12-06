package org.learn.flink.feature.serde;

import org.apache.flink.formats.csv.CsvRowDataDeserializationSchema;
import org.apache.flink.formats.csv.CsvRowDataSerializationSchema;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.InstantiationUtil;

import java.time.Instant;
import java.time.LocalDateTime;

import static org.apache.flink.table.api.DataTypes.*;
import static org.apache.flink.table.data.StringData.fromString;
import static org.apache.flink.table.data.TimestampData.fromInstant;
import static org.apache.flink.table.data.TimestampData.fromLocalDateTime;
import static org.junit.Assert.assertEquals;

public class CsvSerDeExample {
    public static void main(String[] args) throws Exception {
        DataType subDataType0 =
                ROW(
                        FIELD("f0c0", STRING()),
                        FIELD("f0c1", INT()),
                        FIELD("f0c2", STRING()),
                        FIELD("f0c3", TIMESTAMP()),
                        FIELD("f0c4", TIMESTAMP_LTZ()));
        DataType subDataType1 =
                ROW(
                        FIELD("f1c0", STRING()),
                        FIELD("f1c1", INT()),
                        FIELD("f1c2", STRING()),
                        FIELD("f0c3", TIMESTAMP()),
                        FIELD("f0c4", TIMESTAMP_LTZ()));
        DataType dataType = ROW(FIELD("f0", subDataType0), FIELD("f1", subDataType1));
        RowType rowType = (RowType) dataType.getLogicalType();

        // serialization
        CsvRowDataSerializationSchema.Builder serSchemaBuilder =
                new CsvRowDataSerializationSchema.Builder(rowType);
        // deserialization
        CsvRowDataDeserializationSchema.Builder deserSchemaBuilder =
                new CsvRowDataDeserializationSchema.Builder(rowType, InternalTypeInfo.of(rowType));

        RowData originRowData =
                GenericRowData.of(
                        rowData(
                                "hello",
                                1,
                                "This is 1st top column",
                                LocalDateTime.parse("1970-01-01T01:02:03"),
                                Instant.ofEpochMilli(1000)),
                        rowData(
                                "world",
                                2,
                                "This is 2nd top column",
                                LocalDateTime.parse("1970-01-01T01:02:04"),
                                Instant.ofEpochMilli(2000)));

        // we serialize and deserialize the schema to test runtime behavior
        // when the schema is shipped to the cluster
        CsvRowDataSerializationSchema schema =
                InstantiationUtil.deserializeObject(
                        InstantiationUtil.serializeObject(serSchemaBuilder.build()),
                        CsvSerDeExample.class.getClassLoader());
        String serialResult=new String(schema.serialize(originRowData));

        CsvRowDataDeserializationSchema deschema = InstantiationUtil.deserializeObject(
                        InstantiationUtil.serializeObject(deserSchemaBuilder.build()),
                        CsvSerDeExample.class.getClassLoader());

        RowData deserializedRow=deschema.deserialize(serialResult != null ? serialResult.getBytes() : null);

        assertEquals(deserializedRow, originRowData);
    }

    private static RowData rowData(
            String str1, int integer, String str2, LocalDateTime localDateTime, Instant instant) {
        return GenericRowData.of(
                fromString(str1),
                integer,
                fromString(str2),
                fromLocalDateTime(localDateTime),
                fromInstant(instant));
    }
}
