package dev.walgo.dbseeder.writer;

import static org.assertj.core.api.Assertions.assertThat;

import dev.walgo.dbseeder.DBSSettings;
import dev.walgo.dbseeder.PostgreSQLTest;
import dev.walgo.dbseeder.SourceType;
import dev.walgo.dbseeder.data.DataRow;
import dev.walgo.dbseeder.data.ReferenceInfo;
import dev.walgo.dbseeder.data.SeedInfo;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class SQLGeneratorTest_Postgres extends PostgreSQLTest {

    private static DBSSettings settings;

    @BeforeAll
    public static void init() {
        settings = new DBSSettings.Builder()
                .connection(conn)
                .dbSchema("PUBLIC")
                .sourceType(SourceType.CSV)
                .sourceDir("data")
                .build();
    }

    private SeedInfo makeInfo1() {
        SeedInfo info = new SeedInfo();
        info.setTableName("test");
        info.setTableKeys(List.of("id"));
        info.getKeys().put("test_2", 1);

        info.getFields().put("test_1", new SeedInfo.FieldInfo(0));
        info.getFields().put("test_2", new SeedInfo.FieldInfo(1));
        info.getFields().put("test_3", new SeedInfo.FieldInfo(2));
        return info;
    }

    private SeedInfo makeInfo2() {
        SeedInfo info = new SeedInfo();
        info.setTableName("test_2");
        info.getKeys().put("test_2_2", 1);
        info.getFields().put("key_id", new SeedInfo.FieldInfo(0));
        info.getFields().put("test_1_1", new SeedInfo.FieldInfo(1));
        info.getFields().put("test_2_2", new SeedInfo.FieldInfo(2));
        info.getFields().put("test_3_3", new SeedInfo.FieldInfo(3));
        ReferenceInfo ref = new ReferenceInfo();
        ref.setFieldIdx(0);
        ref.setFieldName("test_2");
        ref.setTableKeyColumn("key_id");
        ref.setTableName("test");
        ref.setTableColumn("test_2");
        info.getReferences().put("key_id", ref);
        return info;
    }

    private SeedInfo makeInfo2_MultiRef() {
        SeedInfo info = new SeedInfo();
        info.setTableName("test_2");
        info.getKeys().put("test_2_2", 1);
        info.getFields().put("key_id", new SeedInfo.FieldInfo(0));
        info.getFields().put("test_1_1", new SeedInfo.FieldInfo(1));
        info.getFields().put("test_2_2", new SeedInfo.FieldInfo(2));
        info.getFields().put("test_3_3", new SeedInfo.FieldInfo(3));
        ReferenceInfo ref = new ReferenceInfo();
        ref.setFieldIdx(0);
        ref.setFieldName("test_2");
        ref.setTableKeyColumn("key_id");
        ref.setTableName("test");
        ref.setTableColumn(List.of("test_2", "test_2_sub"));
        info.getReferences().put("key_id", ref);
        return info;
    }

    private DataRow makeTestData(Object... data) {
        return new DataRow(1)
                .addValues(data);
    }

    /**
     * insert
     */

    @Test
    public void testInsertSimple() {
        SeedInfo info = makeInfo1();
        DataRow data = makeTestData("1", "2", "3");
        SQLGenerator instance = new SQLGenerator(List.of(info), settings);
        RequestInfo result = instance.insert(info, data);
        assertThat(result).isNotNull();
        assertThat(result.sql()).isEqualTo("INSERT INTO test (test_1, test_2, test_3) VALUES (?, ?, ?) RETURNING id");
        assertThat(result.data()).containsExactlyElementsOf(data.values());
        List<String> testFields = result.fields().stream().map(RequestInfo.Field::name).toList();
        assertThat(testFields).containsExactlyElementsOf(info.getFields().keySet());
    }

    @Test
    public void testInsertWithDirect() {
        SeedInfo info = makeInfo1();
        DataRow data = makeTestData("1", "!!-2", "3");
        SQLGenerator instance = new SQLGenerator(List.of(info), settings);
        RequestInfo result = instance.insert(info, data);
        assertThat(result).isNotNull();
        assertThat(result.sql()).isEqualTo("INSERT INTO test (test_1, test_2, test_3) VALUES (?, -2, ?) RETURNING id");
        assertThat(result.data()).containsExactlyElementsOf(List.of("1", "3"));
        Map<String, SeedInfo.FieldInfo> fields = new LinkedHashMap<>(info.getFields());
        fields.remove("test_2");
        List<String> testFields = result.fields().stream().map(it -> it.name()).toList();
        assertThat(testFields).containsExactlyElementsOf(fields.keySet());
    }

    @Test
    public void testInsertWithReference() {
        SeedInfo info1 = makeInfo1();
        SeedInfo info2 = makeInfo2();
        DataRow data = makeTestData("0", "1", "2", "3");
        SQLGenerator instance = new SQLGenerator(List.of(info1, info2), settings);
        RequestInfo result = instance.insert(info2, data);
        assertThat(result).isNotNull();
        assertThat(result.sql()).isEqualTo(
                "INSERT INTO test_2 (key_id, test_1_1, test_2_2, test_3_3) VALUES ((SELECT key_id FROM test WHERE test_2 = ?), ?, ?, ?)");
        assertThat(result.data()).containsExactlyElementsOf(data.values());
        List<String> testFields = result.fields().stream().map(it -> it.name()).toList();
        assertThat(testFields).containsExactlyElementsOf(info2.getFields().keySet());
    }

    /**
     * update
     */

    @Test
    public void testUpdateSimple() {
        SeedInfo info = makeInfo1();
        DataRow data = makeTestData("1", "2", "3");
        SQLGenerator instance = new SQLGenerator(List.of(info), settings);
        RequestInfo result = instance.update(info, data);
        assertThat(result).isNotNull();
        assertThat(result.sql()).isEqualTo("UPDATE test SET test_1 = ?, test_3 = ? WHERE test_2 = ?");
        assertThat(result.data()).containsExactlyElementsOf(List.of("1", "3", "2"));
        List<String> testFields = result.fields().stream().map(it -> it.name()).toList();
        assertThat(testFields).containsExactlyElementsOf(List.of("test_1", "test_3", "test_2"));
    }

    @Test
    public void testUpdateWithDirect() {
        SeedInfo info = makeInfo1();
        DataRow data = makeTestData("1", "!!2", "3");
        SQLGenerator instance = new SQLGenerator(List.of(info), settings);
        RequestInfo result = instance.update(info, data);
        assertThat(result).isNotNull();
        assertThat(result.sql()).isEqualTo("UPDATE test SET test_1 = ?, test_3 = ? WHERE test_2 = 2");
        assertThat(result.data()).containsExactlyElementsOf(List.of("1", "3"));
        ArrayList<String> fields = new ArrayList<>(info.getFields().keySet());
        fields.remove("test_2");
        List<String> testFields = result.fields().stream().map(it -> it.name()).toList();
        assertThat(testFields).containsExactlyElementsOf(fields);
    }

    @Test
    public void testUpdateWithReference() {
        SeedInfo info1 = makeInfo1();
        SeedInfo info2 = makeInfo2();
        DataRow data = makeTestData("0", "1", "2", "3");
        SQLGenerator instance = new SQLGenerator(List.of(info1, info2), settings);
        RequestInfo result = instance.update(info2, data);
        assertThat(result).isNotNull();
        assertThat(result.sql()).isEqualTo(
                "UPDATE test_2 SET key_id = (SELECT key_id FROM test WHERE test_2 = ?), test_1_1 = ?, test_3_3 = ? WHERE test_2_2 = ?");
        assertThat(result.data()).containsExactlyElementsOf(List.of("0", "1", "3", "2"));
        List<String> testFields = result.fields().stream().map(it -> it.name()).toList();
        assertThat(testFields).containsExactlyElementsOf(List.of("key_id", "test_1_1", "test_3_3", "test_2_2"));
    }

    @Test
    public void testReference() {
        SeedInfo info1 = makeInfo1();
        SeedInfo info2 = makeInfo2();
        SQLGenerator instance = new SQLGenerator(List.of(info1, info2), settings);
        String fieldName = "key_id";
        String result = instance.reference(info2.getReferences().get(fieldName), "?");
        assertThat(result).isNotNull();
        assertThat(result).isEqualTo("SELECT key_id FROM test WHERE test_2 = ?");
    }

    @Test
    public void testReference_Multy() {
        SeedInfo info1 = makeInfo1();
        SeedInfo info2 = makeInfo2_MultiRef();
        SQLGenerator instance = new SQLGenerator(List.of(info1, info2), settings);
        String fieldName = "key_id";
        String result = instance.reference(info2.getReferences().get(fieldName), "?");
        assertThat(result).isNotNull();
        assertThat(result).isEqualTo("SELECT key_id FROM test WHERE test_2 || '##' || test_2_sub = ?");
    }

    @Test
    public void testCheckRecord() {
        SeedInfo info = makeInfo1();
        DataRow data = makeTestData("1", "2", "3");
        SQLGenerator instance = new SQLGenerator(List.of(info), settings);
        RequestInfo result = instance.checkRecord(info, data);
        assertThat(result).isNotNull();
        assertThat(result.sql()).isEqualTo("SELECT COUNT(*) FROM test WHERE test_2 = ?");
        assertThat(result.data()).containsExactlyElementsOf(List.of("2"));
        List<String> testFields = result.fields().stream().map(it -> it.name()).toList();
        assertThat(testFields).containsExactlyElementsOf(List.of("test_2"));
    }

    /**
     * value2replacement
     */

    @Test
    public void testValue2replacementNull() {
        SQLGenerator instance = new SQLGenerator(List.of(), settings);
        String result = instance.value2replacement(null);
        assertThat(result).isEqualTo(SQLGenerator.DATA_PLACEHOLDER);
    }

    @Test
    public void testValue2replacementPlaceholder() {
        SQLGenerator instance = new SQLGenerator(List.of(), settings);
        String result = instance.value2replacement("test");
        assertThat(result).isEqualTo(SQLGenerator.DATA_PLACEHOLDER);
    }

    @Test
    public void testValue2replacementDirect() {
        SQLGenerator instance = new SQLGenerator(List.of(), settings);
        String data = "test";
        String result = instance.value2replacement(SQLGenerator.DIRECT_VALUE_SIGN + data);
        assertThat(result).isEqualTo(data);
    }

}
