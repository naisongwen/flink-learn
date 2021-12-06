package org.learn.flink.udf;

import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.annotation.InputGroup;
import org.apache.flink.table.functions.ScalarFunction;

public class MaxValueScalarFunction extends ScalarFunction {

    public String eval(@DataTypeHint(inputGroup = InputGroup.ANY) Object... values) {
        Object maxValue = null;
        for (Object v : values) {
            if (maxValue == null || ((Number)v).doubleValue() > ((Number)maxValue).doubleValue())
                maxValue = v;
        }
        return maxValue.toString();
    }

//    public int eval(Integer... values) {
//        return max(values);
//    }
//
//
//    public double eval(Double... values) {
//        return max(values);
//    }

    <T extends Number & Comparable<? super T>> T max(T... values) {
        T maxValue = null;
        for (T v : values) {
            if (maxValue == null || v.compareTo(maxValue) > 0)
                maxValue = v;
        }
        return maxValue;
    }
}
