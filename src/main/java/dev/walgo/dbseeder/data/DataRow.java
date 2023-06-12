package dev.walgo.dbseeder.data;

import java.util.ArrayList;
import java.util.List;

public class DataRow {

    private final int sourceNumber;
    private final List<String> values = new ArrayList<>();

    public DataRow(int sNum) {
        this.sourceNumber = sNum;
    }

    public int sourceNumber() {
        return sourceNumber;
    }

    public List<String> values() {
        return values;
    }

    public DataRow addValues(String... vals) {
        for (String value : vals) {
            values.add(value);
        }
        return this;
    }

    @Override
    public String toString() {
        return "{" + "sourceNumber=" + sourceNumber + ", values=" + values + '}';
    }

}
