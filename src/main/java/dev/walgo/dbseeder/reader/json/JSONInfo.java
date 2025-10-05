package dev.walgo.dbseeder.reader.json;

import dev.walgo.dbseeder.data.ActionType;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.builder.ReflectionToStringBuilder;

public class JSONInfo {

    public String tableName;
    public ActionType action;
    public String extraCondition;

    // Ignore omited fields
    public boolean ignoreOmits;
    public List<String> fields;
    public List<String> keys;
    public List<Reference> references;
    public List<Map<String, Object>> data;

    @Override
    public String toString() {
        return ReflectionToStringBuilder.reflectionToString(this);
    }

    public static class Reference {
        public String fieldName;
        public String tableName;
        public String tableField;

        @Override
        public String toString() {
            return ReflectionToStringBuilder.reflectionToString(this);
        }
    }

}
