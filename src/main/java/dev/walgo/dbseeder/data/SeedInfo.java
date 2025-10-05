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

    // Ignore omited fields
    private boolean ignoreOmits;
    private final Map<String, FieldInfo> fields;
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

    public Object getFieldValue(String fieldName, DataRow row) {
        FieldInfo info = fields.get(fieldName);
        if (info == null) {
            return null;
        }
        return row.values().get(info.index);
    }

    public Object setFieldValue(String fieldName, Object value, DataRow row) {
        FieldInfo info = fields.get(fieldName);
        if (info == null) {
            return null;
        }
        return row.values().set(info.index, value);
    }

    public void setAction(ActionType action) {
        this.action = action;
    }

    public Map<String, FieldInfo> getFields() {
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

    public DataRow getDataRow(int idx) {
        return data.get(idx);
    }

    public Object getDataValue(int idx, String fieldName) {
        FieldInfo info = fields.get(fieldName);
        if (info == null) {
            return null;
        }
        return data.get(idx).values().get(info.index);
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

    public boolean ignoreOmits() {
        return ignoreOmits;
    }

    public void setIgnoreOmits(boolean ignoreOmits) {
        this.ignoreOmits = ignoreOmits;
    }

    @Override
    public String toString() {
        return ReflectionToStringBuilder.reflectionToString(this);
    }

    public static class FieldInfo {
        public int index;
        public boolean isArray; // for non-structured sources, like CSV

        public FieldInfo(int index) {
            this.index = index;
        }

        public FieldInfo(int index, boolean isArray) {
            this.index = index;
            this.isArray = isArray;
        }

    }

}
