package dev.walgo.dbseeder;

import static org.assertj.core.api.Assertions.assertThat;

import dev.walgo.dbseeder.data.ActionType;
import dev.walgo.dbseeder.data.SeedInfo;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.handlers.MapListHandler;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.postgresql.jdbc.PgArray;

public class DBSeederTest_Postgres extends PostgreSQLTest {

//    private static final String TABLE_2 = "test_table_2";
//    private static final String TABLE_1 = "test_table_1";
//    private static final String FIELD_OBJECT = "test_object";
//    private static final String FIELD_ARRAY = "test_array";
//    private static final String FIELD_ENUM = "enum_field";
//    private static final String FIELD_ENUM_2 = "enum_field_2";
//    private static final String FIELD_INT = "read_only";
//    private static final String DECIMAL_FIELD_1 = "decimal_field_1";
//    private static final String DECIMAL_FIELD_2 = "decimal_field_2";
//    private static final String FIELD_ADD_1 = "add_field";
//    private static final String FIELD_ADD_2 = "add_field_2";
//    private static final String FIELD_ADD_3 = "add_field_3";
//    private static final String FIELD_ADD_4 = "add_field_4";
//    private static final String FIELD_ADD_5 = "add_field_5";

    private static final String BIG_FIELD_VALUE = """
        this is
          test
            file.
        """.stripIndent();

//    private static final String TYPE_INTEGER = "Integer";

    @Test
    @Order(10)
    public void testRead() {
        DBSSettings settings = new DBSSettings.Builder()
                .connection(conn)
                .dbSchema(DB_SCHEMA)
                .sourceType(SourceType.CSV)
                .sourceDir("data")
                .build();
        DBSeeder seeder = new DBSeeder(settings);
        seeder.read();
        assertThat(seeder.infos).isNotNull().hasSize(4);

        SeedInfo info1 = seeder.infos.get(1);
        assertThat(info1.getResourceName()).isEqualTo("01--test_table_1.csv");
        assertThat(info1.getTableName()).isEqualTo("test_table_1");
        assertThat(info1.getAction()).isEqualTo(ActionType.MODIFY);
        assertThat(info1.getKeys()).containsExactlyEntriesOf(Map.of("enum_field", 0));
        assertThat(info1.getFields().keySet()).containsExactly("enum_field", "big_field", "read_only", "is_deleted",
                "double_field",
                "char_field", "date_field", "time_field", "timestamp_field", "decimal_field_1", "decimal_field_2",
                "numeric_field",
                "boolean_field", "smallint_field", "bigint_field", "real_field", "binary_field", "varbinary_field",
                "other_field");
        assertThat(info1.getData()).hasSize(2);
    }

    @Test
    @Order(20)
    public void testWrite() throws Exception {

        QueryRunner runner = new QueryRunner();
        runner.execute(conn, "DELETE from test_table_3");
        runner.execute(conn, "DELETE from test_table_2");
        runner.execute(conn, "DELETE from test_table_1");

        final AtomicBoolean isAnythingInserted = new AtomicBoolean(false);
        final Map<String, List<Integer>> insertResult = new HashMap<>();

        DBSSettings settings = new DBSSettings.Builder()
                .connection(conn)
                .dbSchema(DB_SCHEMA)
                .sourceType(SourceType.CSV)
                .sourceDir("data")
                .putOnAfterInsert(DBSSettings.ANY_TABLE, (seed, row, result) -> {
                    isAnythingInserted.set(true);
                    assertThat(result)
                            .isNotNull()
                            .hasSize(1);
                    String key = result.keySet().iterator().next();
                    assertThat(key).isEqualTo("id");
                    Integer value = (Integer) result.values().iterator().next();
                    insertResult.computeIfAbsent(seed.getTableName(), k -> new ArrayList<>()).add(value);
                })
                .build();
        DBSeeder seeder = new DBSeeder(settings);
        seeder.read();
        seeder.write();

        assertThat(isAnythingInserted).isTrue();

        String tableName = "test_table_1";

        List<Map<String, Object>> result = runner.query(conn, "SELECT * from %s".formatted(tableName),
                new MapListHandler());
        assertThat(result).hasSize(2);

        List<Integer> tableInsertResult = insertResult.get(tableName);
        assertThat(tableInsertResult).isNotNull();
        assertThat(result).hasSameSizeAs(tableInsertResult);

        Map<String, Object> row1 = result.get(0);
        assertThat(row1.get("id")).isEqualTo(tableInsertResult.get(0));
        assertThat(row1.get("enum_field")).isEqualTo("TEST1");
        assertThat(row1.get("big_field")).isEqualTo("test_1");
        assertThat(row1.get("read_only")).isEqualTo(10);
        assertThat(row1.get("is_deleted")).isEqualTo(1);
        assertThat(row1.get("double_field")).isEqualTo(11.3);
        assertThat(row1.get("char_field")).isEqualTo("test_ch1  ");
        assertThat(row1.get("date_field")).isEqualTo(Date.valueOf("2023-05-07"));
        assertThat(row1.get("time_field")).isEqualTo(Time.valueOf("13:08:33"));
        assertThat(row1.get("timestamp_field")).isEqualTo(Timestamp.valueOf("2023-05-07 13:08:33.456"));
        assertThat(row1.get("decimal_field_1")).isEqualTo(new BigDecimal(45));
        assertThat(row1.get("decimal_field_2")).isEqualTo(new BigDecimal("45.33"));
        assertThat(row1.get("numeric_field")).isEqualTo(new BigDecimal("34"));
        assertThat(row1.get("boolean_field")).isEqualTo(true);
        assertThat(row1.get("smallint_field")).isEqualTo(20);
        assertThat(row1.get("bigint_field")).isEqualTo(123_456_789L);
        assertThat(row1.get("real_field")).isEqualTo(234.54f);
        assertThat(row1.get("binary_field")).isEqualTo(new byte[] { 0b111111 });
        assertThat(row1.get("varbinary_field")).isEqualTo(new byte[] { 0, -1 });
        assertThat(row1.get("other_field")).isEqualTo("other field");

        Map<String, Object> row2 = result.get(1);
        assertThat(row2.get("id")).isEqualTo(tableInsertResult.get(1));
        assertThat(row2.get("enum_field")).isEqualTo("TEST2");
        assertThat(row2.get("big_field")).isEqualTo("test_2");
        assertThat(row2.get("read_only")).isEqualTo(11);
        assertThat(row2.get("is_deleted")).isEqualTo(2);
        assertThat(row2.get("double_field")).isEqualTo(12.3);
        assertThat(row2.get("char_field")).isEqualTo("test_ch2  ");
        assertThat(row2.get("date_field")).isEqualTo(Date.valueOf("2023-05-08"));
        assertThat(row2.get("time_field")).isEqualTo(Time.valueOf("13:09:33"));
        assertThat(row2.get("timestamp_field")).isEqualTo(Timestamp.valueOf("2023-05-08 13:09:34.456"));
        assertThat(row2.get("decimal_field_1")).isEqualTo(new BigDecimal(46));
        assertThat(row2.get("decimal_field_2")).isEqualTo(new BigDecimal("46.33"));
        assertThat(row2.get("numeric_field")).isEqualTo(new BigDecimal("35"));
        assertThat(row2.get("boolean_field")).isEqualTo(false);
        assertThat(row2.get("smallint_field")).isEqualTo(21);
        assertThat(row2.get("bigint_field")).isEqualTo(123_879_789L);
        assertThat(row2.get("real_field")).isEqualTo(235.54f);
        assertThat(row2.get("binary_field")).isEqualTo(new byte[] { 1, -1 });
        assertThat(row2.get("varbinary_field")).isEqualTo(new byte[] { 7, -1 });
        assertThat(row2.get("other_field")).isEqualTo("other field_2");

        tableName = "test_table_2";
        List<Map<String, Object>> result2 = runner.query(conn, "SELECT * from %s".formatted(tableName),
                new MapListHandler());
        assertThat(result2).hasSize(2);

        tableInsertResult = insertResult.get(tableName);
        assertThat(tableInsertResult).isNotNull();
        assertThat(result2).hasSameSizeAs(tableInsertResult);

        Map<String, Object> row2_1 = result2.get(0);
        assertThat(row2_1.get("id")).isEqualTo(tableInsertResult.get(0));
        assertThat(row2_1.get("enum_field_2")).isEqualTo("TEST11");
        assertThat(row2_1.get("big_field_2")).isEqualTo("test_12");
        assertThat(row2_1.get("read_only")).isEqualTo(1);
        Object array1 = ((PgArray) row2_1.get("test_array")).getArray();
        assertThat(((Object[]) array1)).containsExactly(11, 12, 13);
        Object array2 = ((PgArray) row2_1.get("test_array2")).getArray();
        assertThat(((Object[]) array2)).containsExactly("test_char 1", "test char 2", "test char 3");
        assertThat(row2_1.get("test_object")).isEqualTo("other test");
        assertThat(row2_1.get("is_deleted")).isEqualTo(1);
        assertThat(row2_1.get("test_table_1_id")).isEqualTo(row1.get("id"));

        Map<String, Object> row2_2 = result2.get(1);
        assertThat(row2_2.get("id")).isEqualTo(tableInsertResult.get(1));
        assertThat(row2_2.get("enum_field_2")).isEqualTo("TEST12");
        assertThat(row2_2.get("big_field_2")).isEqualTo("test_13");
        assertThat(row2_2.get("read_only")).isEqualTo(2);
        array1 = ((PgArray) row2_2.get("test_array")).getArray();
        assertThat(((Object[]) array1)).containsExactly(21, 22, 23);
        array2 = ((PgArray) row2_2.get("test_array2")).getArray();
        assertThat(((Object[]) array2)).containsExactly("test_char 11", BIG_FIELD_VALUE, "test char 31");
        assertThat(row2_2.get("test_object")).isEqualTo("other test 11");
        assertThat(row2_2.get("is_deleted")).isEqualTo(0);
        assertThat(row2_2.get("test_table_1_id")).isEqualTo(row2.get("id"));

        tableName = "test_table_3";
        List<Map<String, Object>> result3 = runner.query(conn, "SELECT * from %s".formatted(tableName),
                new MapListHandler());
        assertThat(result3).hasSize(3);

        tableInsertResult = insertResult.get(tableName);
        assertThat(tableInsertResult).isNotNull();
        assertThat(result3).hasSameSizeAs(tableInsertResult);

        Map<String, Object> row3_1 = result3.get(0);
        assertThat(row3_1.get("id")).isEqualTo(tableInsertResult.get(0));
        assertThat(row3_1.get("enum_field_2")).isEqualTo("TEST31");
        assertThat(row3_1.get("big_field_2")).isEqualTo("test_32");
        assertThat(row3_1.get("read_only")).isEqualTo(30);
        assertThat(row3_1.get("is_deleted")).isEqualTo(1);
        assertThat(row3_1.get("test_table_1_id")).isEqualTo(row1.get("id"));
//        assertThat(row3_1.get("test_table_1_id")).isEqualTo(row2_1.get("id"));

        Map<String, Object> row3_2 = result3.get(1);
        assertThat(row3_2.get("id")).isEqualTo(tableInsertResult.get(1));
        assertThat(row3_2.get("enum_field_2")).isEqualTo("TEST32");
        assertThat(row3_2.get("big_field_2")).isEqualTo("test_33");
        assertThat(row3_2.get("read_only")).isEqualTo(31);
        assertThat(row3_2.get("is_deleted")).isEqualTo(0);
        assertThat(row3_2.get("test_table_1_id")).isEqualTo(row2.get("id"));
        assertThat(row3_2.get("test_table_2_id")).isEqualTo(row2_2.get("id"));

        Map<String, Object> row3_3 = result3.get(2);
        assertThat(row3_3.get("id")).isEqualTo(tableInsertResult.get(2));
        assertThat(row3_3.get("enum_field_2")).isEqualTo("TEST33");
        assertThat(row3_3.get("big_field_2")).isEqualTo(BIG_FIELD_VALUE);
        assertThat(row3_3.get("read_only")).isEqualTo(31);
        assertThat(row3_3.get("is_deleted")).isEqualTo(0);
        assertThat(row3_3.get("test_table_1_id")).isEqualTo(row2.get("id"));
        assertThat(row3_3.get("test_table_2_id")).isEqualTo(row2_2.get("id"));

    }

    @Test
    @Order(21)
    public void testWrite2() throws Exception {
        DBSSettings settings = new DBSSettings.Builder()
                .connection(conn)
                .dbSchema(DB_SCHEMA)
                .sourceType(SourceType.CSV)
                .sourceDir("data")
                .build();

        QueryRunner runner = new QueryRunner();
        runner.execute(conn, "DELETE from test_table_3");
        runner.execute(conn, "DELETE from test_table_2");
        runner.execute(conn, "DELETE from test_table_1");

        DBSeeder seeder = new DBSeeder(settings);
        seeder.read();
        seeder.write();

        settings = new DBSSettings.Builder()
                .connection(conn)
                .dbSchema(DB_SCHEMA)
                .sourceType(SourceType.CSV)
                .sourceDir("data")
                .putOnUpdate("test_table_1", (info, row) -> {
                    String fieldName = "big_field";
                    if ("test_1".equals(info.getFieldValue(fieldName, row))) {
                        info.setFieldValue(fieldName, "test_new", row);
                    }
                })
                .putOnRow("test_table_3", (info, row) -> {
                    String fieldName = "test_table_1_id";
                    if ("TEST1##test_1".equals(info.getFieldValue(fieldName, row))) {
                        info.setFieldValue("test_table_1_id", "TEST1##test_new", row);
                    }
                })
                .build();
        seeder = new DBSeeder(settings);
        seeder.read();

//        SeedInfo info1 = seeder.infos.get(0);
//        info1.setFieldValue("big_field", "test_new", info1.getData().get(0));
//
//        SeedInfo info2 = seeder.infos.get(1);
//        info2.setFieldValue("big_field", "test_new", info2.getData().get(0));
//
//        SeedInfo info4 = seeder.infos.get(3);
//        info4.setFieldValue("test_table_1_id", "TEST1##test_new", info4.getData().get(0));
        seeder.write();

        List<Map<String, Object>> result = runner.query(conn, "SELECT * from test_table_1", new MapListHandler());
        assertThat(result).hasSize(2);
        Map<String, Object> row1 = result.get(0);
        assertThat(row1.get("enum_field")).isEqualTo("TEST1");
        assertThat(row1.get("big_field")).isEqualTo("test_new");
        assertThat(row1.get("read_only")).isEqualTo(10);

        Map<String, Object> row2 = result.get(1);
        assertThat(row2.get("enum_field")).isEqualTo("TEST2");
        assertThat(row2.get("big_field")).isEqualTo("test_2");
        assertThat(row2.get("read_only")).isEqualTo(11);

        List<Map<String, Object>> result2 = runner.query(conn, "SELECT * from test_table_2", new MapListHandler());
        assertThat(result2).hasSize(2);
        Map<String, Object> row2_1 = result2.get(0);
        assertThat(row2_1.get("enum_field_2")).isEqualTo("TEST11");
        assertThat(row2_1.get("big_field_2")).isEqualTo("test_12");
        assertThat(row2_1.get("read_only")).isEqualTo(1);
        Object array1 = ((PgArray) row2_1.get("test_array")).getArray();
        assertThat(((Object[]) array1)).containsExactly(11, 12, 13);
        Object array2 = ((PgArray) row2_1.get("test_array2")).getArray();
        assertThat(((Object[]) array2)).containsExactly("test_char 1", "test char 2", "test char 3");
        assertThat(row2_1.get("test_object")).isEqualTo("other test");
        assertThat(row2_1.get("is_deleted")).isEqualTo(1);
        assertThat(row2_1.get("test_table_1_id")).isEqualTo(row1.get("id"));

        Map<String, Object> row2_2 = result2.get(1);
        assertThat(row2_2.get("enum_field_2")).isEqualTo("TEST12");
        assertThat(row2_2.get("big_field_2")).isEqualTo("test_13");
        assertThat(row2_2.get("read_only")).isEqualTo(2);
        array1 = ((PgArray) row2_2.get("test_array")).getArray();
        assertThat(((Object[]) array1)).containsExactly(21, 22, 23);
        array2 = ((PgArray) row2_2.get("test_array2")).getArray();
        assertThat(((Object[]) array2)).containsExactly("test_char 11", BIG_FIELD_VALUE, "test char 31");
        assertThat(row2_2.get("test_object")).isEqualTo("other test 11");
        assertThat(row2_2.get("is_deleted")).isEqualTo(0);
        assertThat(row2_2.get("test_table_1_id")).isEqualTo(row2.get("id"));

        List<Map<String, Object>> result3 = runner.query(conn, "SELECT * from test_table_3 ORDER BY id",
                new MapListHandler());
        assertThat(result3).hasSize(4);
        Map<String, Object> row3_1 = result3.get(0);
        assertThat(row3_1.get("enum_field_2")).isEqualTo("TEST31");
        assertThat(row3_1.get("big_field_2")).isEqualTo("test_32");
        assertThat(row3_1.get("read_only")).isEqualTo(30);
        assertThat(row3_1.get("is_deleted")).isEqualTo(1);
        assertThat(row3_1.get("test_table_1_id")).isEqualTo(row1.get("id"));
        assertThat(row3_1.get("test_table_2_id")).isEqualTo(row2_1.get("id"));

        Map<String, Object> row3_2 = result3.get(1);
        assertThat(row3_2.get("enum_field_2")).isEqualTo("TEST32");
        assertThat(row3_2.get("big_field_2")).isEqualTo("test_33");
        assertThat(row3_2.get("read_only")).isEqualTo(31);
        assertThat(row3_2.get("is_deleted")).isEqualTo(0);
        assertThat(row3_2.get("test_table_1_id")).isEqualTo(row2.get("id"));
        assertThat(row3_2.get("test_table_2_id")).isEqualTo(row2_2.get("id"));

        Map<String, Object> row3_3 = result3.get(2);
        assertThat(row3_3.get("enum_field_2")).isEqualTo("TEST33");
        assertThat(row3_3.get("big_field_2")).isEqualTo(BIG_FIELD_VALUE);
        assertThat(row3_3.get("read_only")).isEqualTo(31);
        assertThat(row3_3.get("is_deleted")).isEqualTo(0);
        assertThat(row3_3.get("test_table_1_id")).isEqualTo(row2.get("id"));
        assertThat(row3_3.get("test_table_2_id")).isEqualTo(row2_2.get("id"));

        Map<String, Object> row3_4 = result3.get(3);
        assertThat(row3_4.get("enum_field_2")).isEqualTo("TEST31");
        assertThat(row3_4.get("big_field_2")).isEqualTo("test_32");
        assertThat(row3_4.get("read_only")).isEqualTo(30);
        assertThat(row3_4.get("is_deleted")).isEqualTo(1);
        assertThat(row3_4.get("test_table_1_id")).isEqualTo(row1.get("id"));
        assertThat(row3_4.get("test_table_2_id")).isEqualTo(row2_1.get("id"));

    }

    @Test
    @Order(30)
    public void testOmited() throws SQLException {
        QueryRunner runner = new QueryRunner();
        runner.execute(conn, "DELETE from test_table_3");
        runner.execute(conn, "DELETE from test_table_2");
        runner.execute(conn, "DELETE from test_table_1");

        DBSSettings settings = new DBSSettings.Builder()
                .connection(conn)
                .dbSchema(DB_SCHEMA)
                .sourceType(SourceType.CSV)
                .sourceDir("data2")
                .build();
        DBSeeder seeder = new DBSeeder(settings);
        seeder.read();
        assertThat(seeder.infos).isNotNull().hasSize(1);

        SeedInfo info1 = seeder.infos.get(0);
        assertThat(info1.getResourceName()).isEqualTo("01--test.omited.csv");
        assertThat(info1.getTableName()).isEqualTo("test_table_1");
        assertThat(info1.getAction()).isEqualTo(ActionType.MODIFY);
        assertThat(info1.getKeys()).containsExactlyEntriesOf(Map.of("enum_field", 0));
        assertThat(info1.getFields().keySet()).containsExactly(
                "enum_field", "big_field", "read_only", "is_deleted", "double_field", "char_field", "date_field",
                "time_field", "timestamp_field", "decimal_field_1");
        assertThat(info1.getData()).hasSize(2);
        assertThat(info1.getDataValue(0, "decimal_field_1")).isEqualTo("45");
        assertThat(info1.getDataValue(1, "decimal_field_1")).isNull();

        seeder.write();

        List<Map<String, Object>> result = runner.query(conn, "SELECT * from test_table_1", new MapListHandler());
        assertThat(result).hasSize(1);
        Map<String, Object> row1 = result.get(0);
        assertThat(row1.get("enum_field")).isEqualTo("TEST1");
        assertThat(row1.get("big_field")).isEqualTo("test_1");
        assertThat(row1.get("read_only")).isEqualTo(10);
        assertThat(row1.get("is_deleted")).isEqualTo(1);
        assertThat(row1.get("double_field")).isEqualTo(11.3);
        assertThat(row1.get("char_field")).isEqualTo("test_ch1  ");
        assertThat(row1.get("date_field")).isEqualTo(Date.valueOf("2023-05-07"));
        assertThat(row1.get("time_field")).isEqualTo(Time.valueOf("13:08:33"));
        assertThat(row1.get("timestamp_field")).isEqualTo(Timestamp.valueOf("2023-05-07 13:08:33.456"));
        assertThat(row1.get("decimal_field_1")).isEqualTo(new BigDecimal("45"));

    }

    @Test
    @Order(40)
    public void testManySources() throws SQLException {
        QueryRunner runner = new QueryRunner();
        runner.execute(conn, "DELETE from test_table_3");
        runner.execute(conn, "DELETE from test_table_2");
        runner.execute(conn, "DELETE from test_table_1");

        DBSSettings settings = new DBSSettings.Builder()
                .connection(conn)
                .dbSchema(DB_SCHEMA)
                .sourceType(SourceType.ANY)
                .sourceDir("data3")
                .build();
        DBSeeder seeder = new DBSeeder(settings);
        seeder.read();
        assertThat(seeder.infos).isNotNull().hasSize(2);

        SeedInfo info1 = seeder.infos.get(0);
        assertThat(info1.getResourceName()).isEqualTo("01--test.csv");
        assertThat(info1.getTableName()).isEqualTo("test_table_1");
        assertThat(info1.getAction()).isEqualTo(ActionType.MODIFY);
        assertThat(info1.getKeys()).containsExactlyEntriesOf(Map.of("enum_field", 0));
        assertThat(info1.getFields().keySet()).containsExactly(
                "enum_field", "big_field", "read_only", "is_deleted", "double_field", "char_field", "date_field",
                "time_field", "timestamp_field", "decimal_field_1");
        assertThat(info1.getData()).hasSize(2);
        assertThat(info1.getDataValue(0, "decimal_field_1")).isEqualTo("44");
        assertThat(info1.getDataValue(1, "decimal_field_1")).isEqualTo("45");

        SeedInfo info2 = seeder.infos.get(1);
        assertThat(info2.getResourceName()).isEqualTo("01--test.json");
        assertThat(info2.getTableName()).isEqualTo("test_table_1");
        assertThat(info2.getAction()).isEqualTo(ActionType.MODIFY);
        assertThat(info2.getKeys()).containsExactlyEntriesOf(Map.of("enum_field", 0));
        assertThat(info2.getFields().keySet()).containsExactly(
                "enum_field", "big_field", "read_only", "is_deleted", "double_field", "char_field", "date_field",
                "time_field", "timestamp_field", "decimal_field_1");

//        SeedInfo info3 = seeder.infos.get(2);
//        assertThat(info3.getResourceName()).isEqualTo("01--test.xml");
//        assertThat(info3.getTableName()).isEqualTo("test_table_1");
//        assertThat(info3.getAction()).isEqualTo(ActionType.MODIFY);
//        assertThat(info3.getKeys()).containsExactlyEntriesOf(Map.of("enum_field", 0));
//        assertThat(info3.getFields().keySet()).containsExactly(
//                "enum_field", "big_field", "read_only", "is_deleted", "double_field", "char_field", "date_field",
//                "time_field", "timestamp_field", "decimal_field_1");

        seeder.write();

        List<Map<String, Object>> result = runner.query(conn, "SELECT * from test_table_1 order by enum_field",
                new MapListHandler());
        assertThat(result).hasSize(2);
        Map<String, Object> row1 = result.get(0);
        assertThat(row1.get("enum_field")).isEqualTo("TEST1");
        assertThat(row1.get("big_field")).isEqualTo("test_1");
        assertThat(row1.get("read_only")).isEqualTo(10);
        assertThat(row1.get("is_deleted")).isEqualTo(1);
        assertThat(row1.get("double_field")).isEqualTo(11.3);
        assertThat(row1.get("char_field")).isEqualTo("test_ch1  ");
        assertThat(row1.get("date_field")).isEqualTo(Date.valueOf("2023-05-07"));
        assertThat(row1.get("time_field")).isEqualTo(Time.valueOf("13:08:33"));
        assertThat(row1.get("timestamp_field")).isEqualTo(Timestamp.valueOf("2023-05-07 13:08:33.456"));
        assertThat(row1.get("decimal_field_1")).isEqualTo(new BigDecimal("45"));

        Map<String, Object> row2 = result.get(1);
        assertThat(row2.get("enum_field")).isEqualTo("TEST2");
        assertThat(row2.get("big_field")).isEqualTo("test_2");
        assertThat(row2.get("read_only")).isEqualTo(11);
        assertThat(row2.get("is_deleted")).isEqualTo(2);
        assertThat(row2.get("double_field")).isEqualTo(12.3);
        assertThat(row2.get("char_field")).isEqualTo("test_ch2  ");
        assertThat(row2.get("date_field")).isEqualTo(Date.valueOf("2023-05-08"));
        assertThat(row2.get("time_field")).isEqualTo(Time.valueOf("13:08:34"));
        assertThat(row2.get("timestamp_field")).isEqualTo(Timestamp.valueOf("2023-05-08 13:08:33.456"));
        assertThat(row2.get("decimal_field_1")).isEqualTo(new BigDecimal("46"));

//        Map<String, Object> row3 = result.get(2);
//        assertThat(row3.get("enum_field")).isEqualTo("TEST3");
//        assertThat(row3.get("big_field")).isEqualTo("test_3");
//        assertThat(row3.get("read_only")).isEqualTo(12);
//        assertThat(row3.get("is_deleted")).isEqualTo(3);
//        assertThat(row3.get("double_field")).isEqualTo(13.3);
//        assertThat(row3.get("char_field")).isEqualTo("test_ch3  ");
//        assertThat(row3.get("date_field")).isEqualTo(Date.valueOf("2023-05-09"));
//        assertThat(row3.get("time_field")).isEqualTo(Time.valueOf("13:08:35"));
//        assertThat(row3.get("timestamp_field")).isEqualTo(Timestamp.valueOf("2023-05-09 13:08:33.456"));
//        assertThat(row3.get("decimal_field_1")).isEqualTo(new BigDecimal("47"));

    }

}
