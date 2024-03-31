package dev.walgo.dbseeder.writer;

import dev.walgo.dbseeder.DBSSettings;
import dev.walgo.dbseeder.data.DataRow;
import dev.walgo.dbseeder.data.ReferenceInfo;
import dev.walgo.dbseeder.data.SeedInfo;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

public class SQLGenerator {

    public static final String DIRECT_VALUE_SIGN = "!!";
    public static final String DATA_PLACEHOLDER = "?";

    private final Map<String, SeedInfo> sources;
    private final DBSSettings settings;

    public SQLGenerator(List<SeedInfo> sources, DBSSettings settings) {
        this.sources = new TreeMap<>();
        for (SeedInfo source : sources) {
            this.sources.put(source.getTableName(), source);
        }
        this.settings = settings;
    }

    public RequestInfo insert(SeedInfo info, DataRow data) {
        final StringBuilder fieldNames = new StringBuilder();
        final StringBuilder insertVars = new StringBuilder();
        RequestInfo.Builder builder = new RequestInfo.Builder();
        info.getFields().forEach((field, idx) -> {
            if (idx > 0) {
                fieldNames.append(", ");
                insertVars.append(", ");
            }
            String value = value2replacement(data.values().get(idx));
            fieldNames.append(field);
            ReferenceInfo ref = info.getReferences().get(field);
            if (ref != null) {
                insertVars.append("(").append(reference(ref, field, value)).append(")");
            } else {
                insertVars.append(value);
            }
            if (isPlaceholder(value)) {
                builder.addData(data.values().get(idx));
                builder.addFields(new RequestInfo.Field(field, idx));
            }
        });
        String sql = "INSERT INTO %s (%s) VALUES (%s)".formatted(info.getTableName(),
                fieldNames.toString(), insertVars.toString());
        if (CollectionUtils.isNotEmpty(info.getTableKeys())) {
            sql += " RETURNING " + StringUtils.join(info.getTableKeys(), ',');
        }
        builder.sql(sql);
        return builder.build();
    }

    public RequestInfo update(SeedInfo info, DataRow data) {
        final StringBuilder updateStr = new StringBuilder();
        final StringBuilder whereString = new StringBuilder();
        List<RequestInfo.Field> whereFields = new ArrayList<>();
        List<String> whereVars = new ArrayList<>();
        RequestInfo.Builder builder = new RequestInfo.Builder();
        info.getFields().forEach((field, idx) -> {
            ReferenceInfo ref = info.getReferences().get(field);
            String placeholder = value2replacement(data.values().get(idx));
            String itemValue = ref == null ? placeholder : "(" + reference(ref, field, placeholder) + ")";
            if (info.getKeys().containsKey(field)) {
                if (!whereString.isEmpty()) {
                    whereString.append(" AND ");
                }
                whereString.append(field).append(" = ").append(itemValue);
                if (isPlaceholder(placeholder)) {
                    whereVars.add(data.values().get(idx));
                    whereFields.add(new RequestInfo.Field(field, idx));
                }
            } else {
                if (!updateStr.isEmpty()) {
                    updateStr.append(", ");
                }
                updateStr.append(field).append(" = ").append(itemValue);
                if (isPlaceholder(placeholder)) {
                    builder.addData(data.values().get(idx));
                    builder.addFields(new RequestInfo.Field(field, idx));
                }
            }
        });
        String result = "UPDATE %s SET %s WHERE %s".formatted(info.getTableName(),
                updateStr.toString(), whereString.toString());
        if ((info.getExtraCondition() != null) && !info.getExtraCondition().isEmpty()) {
            result += " AND " + info.getExtraCondition();
        }
        builder.addAllData(whereVars);
        builder.addAllFields(whereFields);
        builder.sql(result);
        return builder.build();
    }

    public String reference(ReferenceInfo reference, String field, String value) {
        String column = StringUtils.join(reference.getTableColumn(),
                " || '" + settings.csvMultiRefDelimiter() + "' || ");
        String result = "SELECT %s FROM %s WHERE %s = %s".formatted(reference.getTableKeyColumn(),
                reference.getTableName(), column, value);
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
            String checkValue = value2replacement(value);
            boolean usePlaceholder = isPlaceholder(checkValue);
            if (info.getReferences().containsKey(key.getKey())) {
                ReferenceInfo ref = info.getReferences().get(key.getKey());
                String refSql = reference(ref, ref.getFieldName(), checkValue);
                checkValue = '(' + refSql + ')';
            }
            where += key.getKey() + " = " + checkValue;
            if (usePlaceholder) {
                builder.addData(value);
                builder.addFields(new RequestInfo.Field(key.getKey(), info.getFields().get(key.getKey())));
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
