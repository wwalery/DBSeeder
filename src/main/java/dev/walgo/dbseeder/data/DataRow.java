package dev.walgo.dbseeder.data;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class DataRow {

    private final int sourceNumber;
    private final List<Object> values = new ArrayList<>();

    public DataRow(int sNum) {
        this.sourceNumber = sNum;
    }

    public int sourceNumber() {
        return sourceNumber;
    }

    public List<Object> values() {
        return values;
    }

    public DataRow addValue(Object val) {
        values.add(val);
        return this;
    }

    public DataRow addValues(Object... vals) {
        Collections.addAll(values, vals);
        return this;
    }

    @Override
    public String toString() {
        return "{" + "sourceNumber=" + sourceNumber + ", values=" + values + '}';
    }

}
