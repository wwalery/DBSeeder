package dev.walgo.dbseeder.data;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import org.apache.commons.lang3.builder.ReflectionToStringBuilder;

public class SeedInfo {

    private String tableName;

    private String resourceName;
    private ActionType action;
    private String extraCondition;
    private final Map<String, Integer> fields;
    private final Map<String, Integer> keys;
    private final Map<String, ReferenceInfo> references;
    private final List<DataRow> data;
    private List<String> tableKeys;

    public SeedInfo() {
        this.fields = new LinkedHashMap<>();
        this.data = new ArrayList<>();
        this.action = ActionType.IGNORE_NOT_EMPTY;
        this.keys = new TreeMap<>();
        this.references = new HashMap<>();
    }

    public String getFieldValue(String fieldName, DataRow row) {
        Integer idx = fields.get(fieldName);
        if (idx == null) {
            return null;
        }
        return row.values().get(idx);
    }

    public String setFieldValue(String fieldName, String value, DataRow row) {
        Integer idx = fields.get(fieldName);
        if (idx == null) {
            return null;
        }
        return row.values().set(idx, value);
    }

    public void setAction(ActionType action) {
        this.action = action;
    }

    public Map<String, Integer> getFields() {
        return fields;
    }

    public Map<String, Integer> getKeys() {
        return keys;
    }

    public ActionType getAction() {
        return action;
    }

    public Map<String, ReferenceInfo> getReferences() {
        return references;
    }

    public List<DataRow> getData() {
        return data;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String table) {
        this.tableName = table;
    }

    public String getExtraCondition() {
        return extraCondition;
    }

    public void setExtraCondition(String extraCondition) {
        this.extraCondition = extraCondition;
    }

    public String getResourceName() {
        return resourceName;
    }

    public void setResourceName(String resourceName) {
        this.resourceName = resourceName;
    }

    public List<String> getTableKeys() {
        return tableKeys;
    }

    public void setTableKeys(List<String> tableKeys) {
        this.tableKeys = tableKeys;
    }

    @Override
    public String toString() {
        return ReflectionToStringBuilder.reflectionToString(this);
    }

}
