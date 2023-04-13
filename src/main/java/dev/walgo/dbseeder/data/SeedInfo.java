package dev.walgo.dbseeder.data;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public class SeedInfo {

    private String tableName;
    private String resourceName;
    private ActionType action;
    private String extraCondition;
    private final List<String> fields;
    private final Map<String, Integer> keys;
    private final Map<String, ReferenceInfo> references;
    private final List<DataRow> data;

    public SeedInfo() {
        this.fields = new ArrayList<>();
        this.data = new ArrayList<>();
        this.action = ActionType.IGNORE_NOT_EMPTY;
        this.keys = new TreeMap<>();
        this.references = new HashMap<>();
    }

    public void setAction(ActionType action) {
        this.action = action;
    }

    public List<String> getFields() {
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

}
