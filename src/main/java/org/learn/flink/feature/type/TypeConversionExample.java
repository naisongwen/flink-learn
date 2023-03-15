package org.learn.flink.feature.type;

import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.DataType;

public class TypeConversionExample {
    DataType dataType= DataTypes.STRUCTURED(
            Tuple4.class,
            DataTypes.FIELD("valueCount", DataTypes.BIGINT()),
            DataTypes.FIELD("valueType", DataTypes.STRING()),
            DataTypes.FIELD("maxValue",DataTypes.DOUBLE()),
            DataTypes.FIELD("minValue",DataTypes.DOUBLE()));


    public static void main(String[] args) throws Exception {

    }
}
