package dev.walgo.dbseeder.writer;

import dev.walgo.dbseeder.data.DataRow;
import dev.walgo.dbseeder.data.ReferenceInfo;
import dev.walgo.dbseeder.data.SeedInfo;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public class SQLGenerator {

    public static final String DIRECT_VALUE_SIGN = "!!";
    public static final String DATA_PLACEHOLDER = "?";

    private final Map<String, SeedInfo> sources;

    public SQLGenerator(List<SeedInfo> sources) {
        this.sources = new TreeMap<>();
        for (SeedInfo source : sources) {
            this.sources.put(source.getTableName(), source);
        }
    }

    public RequestInfo insert(SeedInfo info, DataRow data) {
        String fieldNames = "";
        String insertVars = "";
        RequestInfo.Builder builder = new RequestInfo.Builder();
        for (int i = 0; i < info.getFields().size(); i++) {
            if (i > 0) {
                fieldNames += ", ";
                insertVars += ", ";
            }
            String value = value2replacement(data.values().get(i));
            String field = info.getFields().get(i);
            fieldNames += field;
            ReferenceInfo ref = info.getReferences().get(field);
            if (ref != null) {
                insertVars += "(" + reference(ref, field, value) + ")";
            } else {
                insertVars += value;
            }
            if (isPlaceholder(value)) {
                builder.addData(data.values().get(i));
                builder.addFields(field);
            }
        }
        String sql = "INSERT INTO %s (%s) VALUES (%s)".formatted(info.getTableName(), fieldNames, insertVars);
        builder.sql(sql);
        return builder.build();
    }

    public RequestInfo update(SeedInfo info, DataRow data) {
        String updateStr = "";
        String whereString = "";
        List<String> whereFields = new ArrayList<>();
        List<String> whereVars = new ArrayList<>();
        RequestInfo.Builder builder = new RequestInfo.Builder();
        for (int i = 0; i < info.getFields().size(); i++) {
            String field = info.getFields().get(i);
            ReferenceInfo ref = info.getReferences().get(field);
            String placeholder = value2replacement(data.values().get(i));
            String itemValue = ref == null ? placeholder : "(" + reference(ref, field, placeholder) + ")";
            if (info.getKeys().containsKey(field)) {
                if (!whereString.isEmpty()) {
                    whereString += " AND ";
                }
                whereString += field + " = " + itemValue;
                if (isPlaceholder(placeholder)) {
                    whereVars.add(data.values().get(i));
                    whereFields.add(field);
                }
            } else {
                if (!updateStr.isEmpty()) {
                    updateStr += ", ";
                }
                updateStr += field + " = " + itemValue;
                if (isPlaceholder(placeholder)) {
                    builder.addData(data.values().get(i));
                    builder.addFields(field);
                }
            }
        }
        String result = "UPDATE %s SET %s WHERE %s".formatted(info.getTableName(), updateStr, whereString);
        if ((info.getExtraCondition() != null) && !info.getExtraCondition().isEmpty()) {
            result += " AND " + info.getExtraCondition();
        }
        builder.addAllData(whereVars);
        builder.addAllFields(whereFields);
        builder.sql(result);
        return builder.build();
    }

    public String reference(ReferenceInfo reference, String field, String value) {
        String result = "SELECT %s FROM %s WHERE %s = %s".formatted(reference.getTableKeyColumn(),
                reference.getTableName(), reference.getTableColumn(), value);
        SeedInfo refInfo = sources.get(reference.getTableName());
        if ((refInfo != null) && (refInfo.getExtraCondition() != null) && !refInfo.getExtraCondition().isEmpty()) {
            result += " AND " + refInfo.getExtraCondition();
        }
        return result;
    }

    public RequestInfo checkRecord(SeedInfo info, DataRow data) {
        RequestInfo.Builder builder = new RequestInfo.Builder();
        String where = "";
        for (Map.Entry<String, Integer> key : info.getKeys().entrySet()) {
            if (!where.isEmpty()) {
                where += " AND ";
            }
            String value = data.values().get(key.getValue());
            String placeholer = value2replacement(value);
            where += key.getKey() + " = " + placeholer;
            if (isPlaceholder(placeholer)) {
                builder.addData(value);
                builder.addFields(key.getKey());
            }
        }
        String result = "SELECT COUNT(*) FROM %s WHERE %s".formatted(info.getTableName(), where);
        if ((info.getExtraCondition() != null) && !info.getExtraCondition().isEmpty()) {
            result += " AND " + info.getExtraCondition();
        }
        builder.sql(result);
        return builder.build();
    }

    /**
     * Replace value by real value.
     *
     * @param data value
     * @return ? as placeholder or real value
     */
    protected String value2replacement(String data) {
        if (data == null) {
            return DATA_PLACEHOLDER;
        } else if (data.startsWith(DIRECT_VALUE_SIGN)) {
            return data.substring(DIRECT_VALUE_SIGN.length());
        } else {
            return DATA_PLACEHOLDER;
        }
    }

    private boolean isPlaceholder(String data) {
        return DATA_PLACEHOLDER.equals(data);
    }

}
