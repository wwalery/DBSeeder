package dev.walgo.dbseeder;

import static org.assertj.core.api.Assertions.assertThat;

import dev.walgo.dbseeder.data.ActionType;
import dev.walgo.dbseeder.data.SeedInfo;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.List;
import java.util.Map;
import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.handlers.MapListHandler;
import org.hsqldb.cmdline.SqlFile;
import org.hsqldb.cmdline.SqlToolError;
import org.hsqldb.jdbc.JDBCArray;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;

public class DBSeederTest {

    private static final String DB_USER = "sa";
    private static final String DB_URL = "jdbc:hsqldb:mem:testdb";
    private static final String TABLE_2 = "test_table_2";
    private static final String TABLE_1 = "test_table_1";
    private static final String FIELD_OBJECT = "test_object";
    private static final String FIELD_ARRAY = "test_array";
    private static final String FIELD_ENUM = "enum_field";
    private static final String FIELD_ENUM_2 = "enum_field_2";
    private static final String FIELD_INT = "read_only";
    private static final String DECIMAL_FIELD_1 = "decimal_field_1";
    private static final String DECIMAL_FIELD_2 = "decimal_field_2";
    private static final String FIELD_ADD_1 = "add_field";
    private static final String FIELD_ADD_2 = "add_field_2";
    private static final String FIELD_ADD_3 = "add_field_3";
    private static final String FIELD_ADD_4 = "add_field_4";
    private static final String FIELD_ADD_5 = "add_field_5";

    private static final String BIG_FIELD_VALUE = """
        this is
          test
            file.
        """.stripIndent();

    private static final String TYPE_INTEGER = "Integer";

    private static Connection conn;

    @BeforeAll
    public static void before() throws SQLException, IOException, SqlToolError {
        conn = DriverManager.getConnection(DB_URL, DB_USER, "");
        try (InputStream inputStream = new FileInputStream("src/test/resources/db/create_db.sql")) {
            SqlFile sqlFile = new SqlFile(
                    new InputStreamReader(inputStream),
                    "init",
                    System.out,
                    "UTF-8",
                    false,
                    new File("."));
            sqlFile.setConnection(conn);
            sqlFile.execute();
        }
    }

    @AfterAll
    public static void after() throws SQLException, IOException {
        conn.close();
        // Files.delete(Paths.get("testdb.log"));
        // Files.delete(Paths.get("testdb.properties"));
        // Files.delete(Paths.get("testdb.script"));
        // Files.delete(Paths.get("testdb.tmp"));
    }

    @Test
    @Order(10)
    public void testRead() throws Exception {
        DBSSettings settings = new DBSSettings.Builder()
                .connection(conn)
                .dbSchema("PUBLIC")
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
        DBSSettings settings = new DBSSettings.Builder()
                .connection(conn)
                .dbSchema("PUBLIC")
                .sourceType(SourceType.CSV)
                .sourceDir("data")
                .build();
        DBSeeder seeder = new DBSeeder(settings);
        seeder.read();
        seeder.write();

        QueryRunner runner = new QueryRunner();
        List<Map<String, Object>> result = runner.query(conn, "SELECT * from test_table_1", new MapListHandler());
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
        assertThat(row1.get("decimal_field_1")).isEqualTo(new BigDecimal(45));
        assertThat(row1.get("decimal_field_2")).isEqualTo(new BigDecimal("45.33"));
        assertThat(row1.get("numeric_field")).isEqualTo(new BigDecimal("34"));
        assertThat(row1.get("boolean_field")).isEqualTo(true);
        assertThat(row1.get("smallint_field")).isEqualTo(20);
        assertThat(row1.get("bigint_field")).isEqualTo(123_456_789L);
        assertThat(row1.get("real_field")).isEqualTo(234.54);
        assertThat(row1.get("binary_field")).isEqualTo(new byte[] { 0b111111, 0 });
        assertThat(row1.get("varbinary_field")).isEqualTo(new byte[] { 0, -1 });
        assertThat(row1.get("other_field")).isEqualTo("other field");

        Map<String, Object> row2 = result.get(1);
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
        assertThat(row2.get("real_field")).isEqualTo(235.54);
        assertThat(row2.get("binary_field")).isEqualTo(new byte[] { 1, -1 });
        assertThat(row2.get("varbinary_field")).isEqualTo(new byte[] { 7, -1 });
        assertThat(row2.get("other_field")).isEqualTo("other field_2");

        List<Map<String, Object>> result2 = runner.query(conn, "SELECT * from test_table_2", new MapListHandler());
        assertThat(result2).hasSize(2);
        Map<String, Object> row2_1 = result2.get(0);
        assertThat(row2_1.get("enum_field_2")).isEqualTo("TEST11");
        assertThat(row2_1.get("big_field_2")).isEqualTo("test_12");
        assertThat(row2_1.get("read_only")).isEqualTo(1);
        Object array1 = ((JDBCArray) row2_1.get("test_array")).getArray();
        assertThat(((Object[]) array1)).containsExactly(11, 12, 13);
        Object array2 = ((JDBCArray) row2_1.get("test_array2")).getArray();
        assertThat(((Object[]) array2)).containsExactly("test_char 1", "test char 2", "test char 3");
        assertThat(row2_1.get("test_object")).isEqualTo("other test");
        assertThat(row2_1.get("is_deleted")).isEqualTo(1);
        assertThat(row2_1.get("test_table_1_id")).isEqualTo(row1.get("id"));

        Map<String, Object> row2_2 = result2.get(1);
        assertThat(row2_2.get("enum_field_2")).isEqualTo("TEST12");
        assertThat(row2_2.get("big_field_2")).isEqualTo("test_13");
        assertThat(row2_2.get("read_only")).isEqualTo(2);
        array1 = ((JDBCArray) row2_2.get("test_array")).getArray();
        assertThat(((Object[]) array1)).containsExactly(21, 22, 23);
        array2 = ((JDBCArray) row2_2.get("test_array2")).getArray();
        assertThat(((Object[]) array2)).containsExactly("test_char 11", BIG_FIELD_VALUE, "test char 31");
        assertThat(row2_2.get("test_object")).isEqualTo("other test 11");
        assertThat(row2_2.get("is_deleted")).isEqualTo(0);
        assertThat(row2_2.get("test_table_1_id")).isEqualTo(row2.get("id"));

        List<Map<String, Object>> result3 = runner.query(conn, "SELECT * from test_table_3", new MapListHandler());
        assertThat(result3).hasSize(3);
        Map<String, Object> row3_1 = result3.get(0);
        assertThat(row3_1.get("enum_field_2")).isEqualTo("TEST31");
        assertThat(row3_1.get("big_field_2")).isEqualTo("test_32");
        assertThat(row3_1.get("read_only")).isEqualTo(30);
        assertThat(row3_1.get("is_deleted")).isEqualTo(1);
        assertThat(row3_1.get("test_table_1_id")).isEqualTo(row1.get("id"));
        assertThat(row3_1.get("test_table_1_id")).isEqualTo(row2_1.get("id"));

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

    }

    @Test
    @Order(21)
    public void testWrite2() throws Exception {
        DBSSettings settings = new DBSSettings.Builder()
                .connection(conn)
                .dbSchema("PUBLIC")
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
                .dbSchema("PUBLIC")
                .sourceType(SourceType.CSV)
                .sourceDir("data")
                .putOnUpdate("test_table_1", (info, row) -> {
                    String fieldName = "big_field";
                    if ("test_1".equals(info.getFieldValue(fieldName, row))) {
                        info.setFieldValue(fieldName, "test_new", row);
                    }
                })
                .putOnInsert("test_table_3", (info, row) -> {
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
        Object array1 = ((JDBCArray) row2_1.get("test_array")).getArray();
        assertThat(((Object[]) array1)).containsExactly(11, 12, 13);
        Object array2 = ((JDBCArray) row2_1.get("test_array2")).getArray();
        assertThat(((Object[]) array2)).containsExactly("test_char 1", "test char 2", "test char 3");
        assertThat(row2_1.get("test_object")).isEqualTo("other test");
        assertThat(row2_1.get("is_deleted")).isEqualTo(1);
        assertThat(row2_1.get("test_table_1_id")).isEqualTo(row1.get("id"));

        Map<String, Object> row2_2 = result2.get(1);
        assertThat(row2_2.get("enum_field_2")).isEqualTo("TEST12");
        assertThat(row2_2.get("big_field_2")).isEqualTo("test_13");
        assertThat(row2_2.get("read_only")).isEqualTo(2);
        array1 = ((JDBCArray) row2_2.get("test_array")).getArray();
        assertThat(((Object[]) array1)).containsExactly(21, 22, 23);
        array2 = ((JDBCArray) row2_2.get("test_array2")).getArray();
        assertThat(((Object[]) array2)).containsExactly("test_char 11", BIG_FIELD_VALUE, "test char 31");
        assertThat(row2_2.get("test_object")).isEqualTo("other test 11");
        assertThat(row2_2.get("is_deleted")).isEqualTo(0);
        assertThat(row2_2.get("test_table_1_id")).isEqualTo(row2.get("id"));

        List<Map<String, Object>> result3 = runner.query(conn, "SELECT * from test_table_3", new MapListHandler());
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

}
