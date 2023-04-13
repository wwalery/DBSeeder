package dev.walgo.dbseeder.writer;

import static org.assertj.core.api.Assertions.assertThat;

import dev.walgo.dbseeder.data.DataRow;
import dev.walgo.dbseeder.data.ReferenceInfo;
import dev.walgo.dbseeder.data.SeedInfo;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

public class SQLGeneratorTest {

    private SeedInfo makeInfo1() {
        SeedInfo info = new SeedInfo();
        info.setTableName("test");
        info.getKeys().put("test_2", 1);
        info.getFields().addAll(List.of("test_1", "test_2", "test_3"));
        return info;
    }

    private SeedInfo makeInfo2() {
        SeedInfo info = new SeedInfo();
        info.setTableName("test_2");
        info.getKeys().put("test_2_2", 1);
        info.getFields().addAll(List.of("key_id", "test_1_1", "test_2_2", "test_3_3"));
        ReferenceInfo ref = new ReferenceInfo();
        ref.setFieldIdx(0);
        ref.setFieldName("key_id");
        ref.setTableName("test");
        ref.setTableColumn("test_2");
        info.getReferences().put("key_id", ref);
        return info;
    }

    private DataRow makeTestData(String... data) {
        return new DataRow.Builder()
                .sourceNumber(1)
                .addValues(data)
                .build();
    }

    /**
     * insert
     */

    @Test
    public void testInsertSimple() {
        SeedInfo info = makeInfo1();
        DataRow data = makeTestData("1", "2", "3");
        SQLGenerator instance = new SQLGenerator(List.of(info));
        RequestInfo result = instance.insert(info, data);
        assertThat(result).isNotNull();
        assertThat(result.sql()).isEqualTo("INSERT INTO test (test_1, test_2, test_3) VALUES (?, ?, ?)");
        assertThat(result.data()).containsExactlyElementsOf(data.values());
        assertThat(result.fields()).containsExactlyElementsOf(info.getFields());
    }

    @Test
    public void testInsertWithDirect() {
        SeedInfo info = makeInfo1();
        DataRow data = makeTestData("1", "!!-2", "3");
        SQLGenerator instance = new SQLGenerator(List.of(info));
        RequestInfo result = instance.insert(info, data);
        assertThat(result).isNotNull();
        assertThat(result.sql()).isEqualTo("INSERT INTO test (test_1, test_2, test_3) VALUES (?, -2, ?)");
        assertThat(result.data()).containsExactlyElementsOf(List.of("1", "3"));
        ArrayList<String> fields = new ArrayList<>(info.getFields());
        fields.remove("test_2");
        assertThat(result.fields()).containsExactlyElementsOf(fields);
    }

    @Test
    public void testInsertWithReference() {
        SeedInfo info1 = makeInfo1();
        SeedInfo info2 = makeInfo2();
        DataRow data = makeTestData("0", "1", "2", "3");
        SQLGenerator instance = new SQLGenerator(List.of(info1, info2));
        RequestInfo result = instance.insert(info2, data);
        assertThat(result).isNotNull();
        assertThat(result.sql()).isEqualTo(
                "INSERT INTO test_2 (key_id, test_1_1, test_2_2, test_3_3) VALUES ((SELECT key_id FROM test WHERE test_2 = ?), ?, ?, ?)");
        assertThat(result.data()).containsExactlyElementsOf(data.values());
        assertThat(result.fields()).containsExactlyElementsOf(info2.getFields());
    }

    /**
     * update
     */

    @Test
    public void testUpdateSimple() {
        SeedInfo info = makeInfo1();
        DataRow data = makeTestData("1", "2", "3");
        SQLGenerator instance = new SQLGenerator(List.of(info));
        RequestInfo result = instance.update(info, data);
        assertThat(result).isNotNull();
        assertThat(result.sql()).isEqualTo("UPDATE test SET test_1 = ?, test_3 = ? WHERE test_2 = ?");
        assertThat(result.data()).containsExactlyElementsOf(List.of("1", "3", "2"));
        assertThat(result.fields()).containsExactlyElementsOf(List.of("test_1", "test_3", "test_2"));
    }

    @Test
    public void testUpdateWithDirect() {
        SeedInfo info = makeInfo1();
        DataRow data = makeTestData("1", "!!2", "3");
        SQLGenerator instance = new SQLGenerator(List.of(info));
        RequestInfo result = instance.update(info, data);
        assertThat(result).isNotNull();
        assertThat(result.sql()).isEqualTo("UPDATE test SET test_1 = ?, test_3 = ? WHERE test_2 = 2");
        assertThat(result.data()).containsExactlyElementsOf(List.of("1", "3"));
        ArrayList<String> fields = new ArrayList<>(info.getFields());
        fields.remove("test_2");
        assertThat(result.fields()).containsExactlyElementsOf(fields);
    }

    @Test
    public void testUpdateWithReference() {
        SeedInfo info1 = makeInfo1();
        SeedInfo info2 = makeInfo2();
        DataRow data = makeTestData("0", "1", "2", "3");
        SQLGenerator instance = new SQLGenerator(List.of(info1, info2));
        RequestInfo result = instance.update(info2, data);
        assertThat(result).isNotNull();
        assertThat(result.sql()).isEqualTo(
                "UPDATE test_2 SET key_id = (SELECT key_id FROM test WHERE test_2 = ?), test_1_1 = ?, test_3_3 = ? WHERE test_2_2 = ?");
        assertThat(result.data()).containsExactlyElementsOf(List.of("0", "1", "3", "2"));
        assertThat(result.fields()).containsExactlyElementsOf(List.of("key_id", "test_1_1", "test_3_3", "test_2_2"));
    }

    @Test
    public void testReference() {
        SeedInfo info1 = makeInfo1();
        SeedInfo info2 = makeInfo2();
        SQLGenerator instance = new SQLGenerator(List.of(info1, info2));
        String fieldName = "key_id";
        String result = instance.reference(info2.getReferences().get(fieldName), fieldName, "?");
        assertThat(result).isNotNull();
        assertThat(result).isEqualTo("SELECT key_id FROM test WHERE test_2 = ?");
    }

    @Test
    public void testCheckRecord() {
        SeedInfo info = makeInfo1();
        DataRow data = makeTestData("1", "2", "3");
        SQLGenerator instance = new SQLGenerator(List.of(info));
        RequestInfo result = instance.checkRecord(info, data);
        assertThat(result).isNotNull();
        assertThat(result.sql()).isEqualTo("SELECT COUNT(*) FROM test WHERE test_2 = ?");
        assertThat(result.data()).containsExactlyElementsOf(List.of("2"));
        assertThat(result.fields()).containsExactlyElementsOf(List.of("test_2"));
    }

    /**
     * value2replacement
     */

    @Test
    public void testValue2replacementNull() {
        SQLGenerator instance = new SQLGenerator(List.of());
        String result = instance.value2replacement(null);
        assertThat(result).isEqualTo(SQLGenerator.DATA_PLACEHOLDER);
    }

    @Test
    public void testValue2replacementPlaceholder() {
        SQLGenerator instance = new SQLGenerator(List.of());
        String result = instance.value2replacement("test");
        assertThat(result).isEqualTo(SQLGenerator.DATA_PLACEHOLDER);
    }

    @Test
    public void testValue2replacementDirect() {
        SQLGenerator instance = new SQLGenerator(List.of());
        String data = "test";
        String result = instance.value2replacement(SQLGenerator.DIRECT_VALUE_SIGN + data);
        assertThat(result).isEqualTo(data);
    }

}
