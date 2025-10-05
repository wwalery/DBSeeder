package dev.walgo.dbseeder.reader.json;

import com.fasterxml.jackson.core.StreamReadConstraints;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import dev.walgo.dbseeder.DBSSettings;
import dev.walgo.dbseeder.data.DataRow;
import dev.walgo.dbseeder.data.ReferenceInfo;
import dev.walgo.dbseeder.data.SeedInfo;
import dev.walgo.dbseeder.reader.IReader;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map;

public class JSONReader implements IReader {

    private static ObjectMapper mapper;

    private final DBSSettings settings;

    public JSONReader(DBSSettings settings) {
        this.settings = settings;
    }

    private static ObjectMapper getMapper() {
        if (mapper == null) {
            mapper = new ObjectMapper();
            // Serialization & deserialization settings
            mapper.configure(DeserializationFeature.FAIL_ON_NUMBERS_FOR_ENUMS, true);
            mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, true);
            mapper.configure(DeserializationFeature.ACCEPT_SINGLE_VALUE_AS_ARRAY, true);

            // Stream read constraints
            StreamReadConstraints constraints = StreamReadConstraints.builder()
                    .maxStringLength(Integer.MAX_VALUE)
                    .build();
            mapper.getFactory().setStreamReadConstraints(constraints);

//            mapper.setVisibility(PropertyAccessor.ALL, Visibility.NONE);
//            mapper.setVisibility(PropertyAccessor.FIELD, Visibility.ANY);
        }
        return mapper;
    }

    @Override
    public SeedInfo read(String resourceName, InputStream input) throws IOException {
        JSONInfo data = getMapper().reader().readValue(input, JSONInfo.class);
        SeedInfo seed = new SeedInfo();
        seed.setTableName(data.tableName);
        seed.setResourceName(resourceName);
        seed.setAction(data.action);
        seed.setExtraCondition(data.extraCondition);
        seed.setIgnoreOmits(data.ignoreOmits);

        Map<String, SeedInfo.FieldInfo> fields = seed.getFields();
        for (int i = 0; i < data.fields.size(); i++) {
            String field = data.fields.get(i);
            SeedInfo.FieldInfo info = new SeedInfo.FieldInfo(i);
            fields.put(field, info);
        }
        if (data.keys != null) {
            Map<String, Integer> keys = seed.getKeys();
            for (int i = 0; i < data.keys.size(); i++) {
                String key = data.keys.get(i);
                keys.put(key, i);
            }
        }

        if (data.references != null) {
            Map<String, ReferenceInfo> references = seed.getReferences();
            for (int i = 0; i < data.references.size(); i++) {
                JSONInfo.Reference ref = data.references.get(i);
                ReferenceInfo reference = new ReferenceInfo();
                reference.setFieldName(ref.fieldName);
                reference.setTableName(ref.tableName);
                reference.setTableColumn(ref.tableField);
                int fieldIdx = fields.get(ref.fieldName).index;
                reference.setFieldIdx(fieldIdx);
                references.put(ref.fieldName, reference);
            }
        }

        for (int i = 0; i < data.data.size(); i++) {
            Map<String, Object> origDataRow = data.data.get(i);
            if (!seed.ignoreOmits() && (origDataRow.size() != fields.size())) {
                throw new RuntimeException("There is [%s] columns in field list but [%s] in item [%s]"
                        .formatted(data.fields.size(), origDataRow.size(), i));
            }
            DataRow row = new DataRow(i);
            Object[] rowObjects = new Object[origDataRow.size()];
            for (Map.Entry<String, Object> entry : origDataRow.entrySet()) {
                SeedInfo.FieldInfo fieldInfo = seed.getFields().get(entry.getKey());
                if (fieldInfo == null) {
                    throw new RuntimeException(
                            "Unknown field [%s] in item [%s]".formatted(entry.getKey(), origDataRow));
                }
                rowObjects[fieldInfo.index] = entry.getValue();
            }
            row.addValues(rowObjects);
            seed.getData().add(row);
        }

        return seed;

    }

}
