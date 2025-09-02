package dev.walgo.dbseeder.writer;

import dev.walgo.dbseeder.DBSSettings;
import dev.walgo.dbseeder.data.ActionType;
import dev.walgo.dbseeder.data.DataRow;
import dev.walgo.dbseeder.data.ReferenceInfo;
import dev.walgo.dbseeder.data.SeedInfo;
import dev.walgo.dbseeder.db.Database;
import dev.walgo.dbseeder.db.UnknownDatabase;
import dev.walgo.walib.db.ColumnInfo;
import dev.walgo.walib.db.DBInfo;
import dev.walgo.walib.db.DBUtils;
import dev.walgo.walib.db.TableInfo;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Date;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.ResultSetHandler;
import org.apache.commons.dbutils.handlers.MapHandler;
import org.apache.commons.dbutils.handlers.ScalarHandler;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.function.TriConsumer;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DBWriter implements IWriter {

    private static final Logger LOG = LoggerFactory.getLogger(DBWriter.class);
    private static final QueryRunner SQL = new QueryRunner();
    private static final ResultSetHandler<Map<String, Object>> insertHandler = new MapHandler();

    private final List<SeedInfo> infos;
    // private final String schema;
    private final DBSSettings settings;
    private final DBInfo dbInfo;
    private Database database;

    public DBWriter(List<SeedInfo> infos, DBSSettings settings) {
        this.infos = infos;
//        this.schema = schema;
        this.settings = settings;
        this.dbInfo = new DBInfo(settings.connection(), null, settings.dbSchema(), null);
        final ServiceLoader<Database> databases = ServiceLoader.load(Database.class);
        String url = null;
        try {
            url = settings.connection().getMetaData().getURL();
        } catch (SQLException ex) {
            LOG.error("Can't find database URL", ex);
        }
        if (url != null) {
            for (Database db : databases) {
                if (db.handlesJDBCUrl(url)) {
                    this.database = db;
                    break;
                }
            }
        }
        if (this.database == null) {
            this.database = new UnknownDatabase();
        }
        this.database.setConnection(settings.connection());
    }

    private void checkSeed(SeedInfo info) {
        try {
            Map<String, TableInfo> tableMap = dbInfo.getTablesAsMap();
            TableInfo table = tableMap.get(info.getTableName().toLowerCase(Locale.ROOT));
            if (table == null) {
                throw new RuntimeException("Table [%s] doesn't exist".formatted(info.getTableName()));
            }
            if (database.insertHasReturning()) {
                info.setTableKeys(table.getKeys());
            }
            Map<String, ColumnInfo> fields = table.getFields();
            info.getFields().forEach((dataField, idx) -> {
                try {
                    String fieldName = dataField.toLowerCase(Locale.ROOT);
                    ColumnInfo field = fields.get(fieldName);
                    if (field == null) {
                        throw new RuntimeException(
                                "Field N %s [%s] doesn't exists in table [%s]".formatted(idx, dataField,
                                        info.getTableName()));
                    }
                    if (info.getReferences().containsKey(fieldName)) {
                        ReferenceInfo ref = info.getReferences().get(fieldName);
                        TableInfo refTable = tableMap.get(ref.getTableName().toLowerCase(Locale.ROOT));
                        if (refTable == null) {
                            throw new RuntimeException(
                                    "Reference table [%s] doesn't exist".formatted(ref.getTableName()));
                        }
                        for (String refColumn : ref.getTableColumn()) {
                            field = refTable.getFields().get(refColumn.toLowerCase(Locale.ROOT));
                            if (field == null) {
                                throw new RuntimeException(
                                        "Field [%s] doesn't exists in reference table [%s]".formatted(
                                                ref.getTableColumn(),
                                                ref.getTableName()));
                            }
                            if (refTable.getKeys().isEmpty()) {
                                throw new RuntimeException(
                                        "Referenced table [%s] hasn't key columns".formatted(ref.getTableName()));
                            }
//                            if (refTable.getKeys().size() > 1) {
//                                throw new RuntimeException(
//                                        "Referenced table [%s] has more than one key columns"
//                                                .formatted(ref.getTableName()));
//                            }
                        }
                        ref.setTableKeyColumn(refTable.getKeys().get(0));
                    }
                } catch (SQLException ex) {
                    throw new RuntimeException(ex);
                }
            });
            int i = 1;
            for (String dataField : info.getKeys().keySet()) {
                String fieldName = dataField.toLowerCase(Locale.ROOT);
                ColumnInfo field = fields.get(fieldName);
                if (field == null) {
                    throw new RuntimeException(
                            "Key N %s [%s] doesn't exists in table [%s]".formatted(i, dataField, info.getTableName()));
                }
                i++;
            }
        } catch (SQLException ex) {
            throw new RuntimeException(ex);
        }

    }

    private void onEvent(Map<String, BiConsumer<SeedInfo, DataRow>> events, SeedInfo info, DataRow row) {
        if (events == null) {
            return;
        }
        BiConsumer<SeedInfo, DataRow> event = events.getOrDefault(info.getTableName(),
                events.get(DBSSettings.ANY_TABLE));
        if (event == null) {
            return;
        }
        event.accept(info, row);
    }

    private void onEvent(Map<String, Consumer<SeedInfo>> events, SeedInfo info) {
        if (events == null) {
            return;
        }
        Consumer<SeedInfo> event = events.getOrDefault(info.getTableName(), events.get(DBSSettings.ANY_TABLE));
        if (event == null) {
            return;
        }
        event.accept(info);
    }

    private void onEvent(Map<String, TriConsumer<SeedInfo, DataRow, Map<String, Object>>> events, SeedInfo info,
            DataRow row, Map<String, Object> result) {
        if (events == null) {
            return;
        }
        TriConsumer<SeedInfo, DataRow, Map<String, Object>> event = events.getOrDefault(info.getTableName(),
                events.get(DBSSettings.ANY_TABLE));
        if (event == null) {
            return;
        }
        event.accept(info, row, result);
    }

    @Override
    public Pair<Integer, Integer> write(SeedInfo info) {
        LOG.info("Process table: [{}], resource: [{}]", info.getTableName(), info.getResourceName());
        onEvent(settings.onStartData(), info);
        checkSeed(info);
        try {
            switch (info.getAction()) {
                case IGNORE:
                    onEvent(settings.onEndData(), info);
                    return Pair.of(0, 0);
                case IGNORE_NOT_EMPTY:
                    Number count = SQL.query(settings.connection(),
                            "SELECT COUNT(*) FROM %s".formatted(info.getTableName()),
                            new ScalarHandler<>());
                    if (count.intValue() > 0) {
                        onEvent(settings.onEndData(), info);
                        return Pair.of(0, 0);
                    }
                    break;
                default:
                    // do nothing
            }
            SQLGenerator generator = new SQLGenerator(infos, settings);
            int inserted = 0;
            int updated = 0;
            for (DataRow data : info.getData()) {
                onEvent(settings.onRow(), info, data);
                try {
                    boolean recordExists = false;
                    if (info.getAction() != ActionType.IGNORE_NOT_EMPTY) {
                        RequestInfo checkData = generator.checkRecord(info, data);
                        Number records = query(info, checkData);
                        recordExists = records.intValue() > 0;
                    }
                    switch (info.getAction()) {
                        case INSERT:
                        case IGNORE_NOT_EMPTY:
                            if (!recordExists) {
                                onEvent(settings.onInsert(), info, data);
                                RequestInfo insertData = generator.insert(info, data);
                                Map<String, Object> insertResult = insert(info, insertData);
                                if (database.insertHasReturning()) {
                                    onEvent(settings.onAfterInsert(), info, data, insertResult);
                                }
                                inserted++;
                            }
                            break;
                        case MODIFY:
                            if (!recordExists) {
                                onEvent(settings.onInsert(), info, data);
                                RequestInfo insertData = generator.insert(info, data);
                                Map<String, Object> insertResult = insert(info, insertData);
                                if (database.insertHasReturning()) {
                                    onEvent(settings.onAfterInsert(), info, data, insertResult);
                                }
                                inserted++;
                            } else {
                                onEvent(settings.onUpdate(), info, data);
                                RequestInfo updateData = generator.update(info, data);
                                if (updateData != null) {
                                    updated += update(info, updateData);
                                }
                            }
                            break;
                        default:
                            LOG.warn("Undefined action [{}]", info.getAction());
                    }
                } catch (Throwable e) {
                    LOG.error("Error on file [{}], line [{}]: {}", info.getResourceName(), data.sourceNumber(),
                            e.getMessage(), e);
                    throw e;
                }
            }
            onEvent(settings.onEndData(), info);
            return Pair.of(inserted, updated);
        } catch (SQLException ex) {
            throw new RuntimeException(ex);
        }
    }

    @SuppressWarnings("TypeParameterUnusedInFormals")
    private <T> T query(SeedInfo info, RequestInfo requestData) {
        Object[] data = requestDataTypefication(info, requestData);
        if (LOG.isTraceEnabled()) {
            LOG.trace("{} -> {}", requestData.sql(), Arrays.toString(data));
        }
        try {
            return SQL.query(settings.connection(), requestData.sql(), new ScalarHandler<>(), data);
        } catch (SQLException ex) {
            throw new RuntimeException(ex);
        }
    }

    private Map<String, Object> insert(SeedInfo info, RequestInfo requestData) {
        Object[] data = requestDataTypefication(info, requestData);
        if (LOG.isTraceEnabled()) {
            LOG.trace("{} -> {}", requestData.sql(), Arrays.toString(data));
        }
        try {
            return SQL.insert(settings.connection(), requestData.sql(), insertHandler, data);
        } catch (SQLException ex) {
            throw new RuntimeException(ex);
        }
    }

    private int update(SeedInfo info, RequestInfo requestData) {
        Object[] data = requestDataTypefication(info, requestData);
        if (LOG.isTraceEnabled()) {
            LOG.trace("{} -> {}", requestData.sql(), Arrays.toString(data));
        }
        try {
            return SQL.execute(settings.connection(), requestData.sql(), data);
        } catch (SQLException ex) {
            throw new RuntimeException(ex);
        }
    }

    private Object[] requestDataTypefication(SeedInfo info, RequestInfo requestData) {
        List<Object> data = new ArrayList<>();
        try {
            Map<String, TableInfo> tableMap = dbInfo.getTablesAsMap();
            TableInfo table = tableMap.get(info.getTableName().toLowerCase(Locale.ROOT));
            Map<String, ColumnInfo> fields = table.getFields();
            for (int i = 0; i < requestData.fields().size(); i++) {
                RequestInfo.Field dataField = requestData.fields().get(i);
                String fieldName = dataField.name().toLowerCase(Locale.ROOT);
                ColumnInfo field = fields.get(fieldName);
                String stringItem = requestData.data().get(i);
                Object dataItem;
                if (info.getReferences().containsKey(fieldName)) {
                    ReferenceInfo ref = info.getReferences().get(fieldName);
                    TableInfo refTable = tableMap.get(ref.getTableName().toLowerCase(Locale.ROOT));
                    if (ref.getTableColumn().size() == 1) {
                        field = refTable.getFields().get(ref.getTableColumn().get(0).toLowerCase(Locale.ROOT));
                        dataItem = string2object(stringItem, field, dataField);
                    } else {
                        dataItem = stringItem;
                    }
                } else {
                    dataItem = string2object(stringItem, field, dataField);
                }
                data.add(dataItem);
            }
            return data.toArray(Object[]::new);
        } catch (SQLException ex) {
            throw new RuntimeException(ex);
        }

    }

    private String checkExternal(String item) {
        if (!item.startsWith(settings.externalValueRef())) {
            return item;
        }
        String fileName = item.substring(settings.externalValueRef().length());
        String srcDir = settings.sourceDir().endsWith("/") ? settings.sourceDir() : settings.sourceDir() + "/";
        File dir = new File(srcDir);
        boolean isExternalResource = dir.exists();
        if (isExternalResource) {
            Path path = new File(srcDir + fileName).toPath();
            try {
                return Files.readString(path);
            } catch (IOException ex) {
                throw new RuntimeException(ex);
            }
        } else {
            ClassLoader classLoader = settings.classLoader() != null
                    ? settings.classLoader()
                    : getClass().getClassLoader();
            try (InputStream stream = classLoader.getResourceAsStream(srcDir + fileName)) {
                return new String(stream.readAllBytes(), StandardCharsets.UTF_8);
            } catch (Exception ex) {
                LOG.error("Error on reading resource: [{}]", srcDir + fileName);
                throw new RuntimeException(ex);
            }
        }
    }

    protected Object string2object(String stringItem, int fieldType, String fieldTypeName) {
        Object dataItem;
        if (stringItem == null) {
            return null;
        } else if (DBUtils.isStringField(fieldType)) {
            dataItem = checkExternal(stringItem);
        } else {
            dataItem = switch (fieldType) {
                case Types.SMALLINT,
                        Types.INTEGER ->
                    Integer.valueOf(stringItem);
                case Types.BIGINT -> Long.valueOf(stringItem);
                case Types.BOOLEAN, Types.BIT -> Boolean.valueOf(stringItem);
                case Types.DATE -> Date.valueOf(stringItem);
                case Types.DECIMAL, Types.DOUBLE, Types.FLOAT, Types.NUMERIC, Types.REAL, Types.TINYINT ->
                    new BigDecimal(stringItem);
                case Types.TIME -> Time.valueOf(stringItem);
                case Types.TIMESTAMP -> stringItem.contains("T")
                        ? Timestamp.from(Instant.parse(stringItem))
                        : Timestamp.valueOf(stringItem);
                case Types.TIMESTAMP_WITH_TIMEZONE,
                        Types.TIME_WITH_TIMEZONE ->
                    ZonedDateTime.parse(stringItem);
                case Types.BINARY, Types.VARBINARY -> new BigInteger(stringItem, 2).toByteArray();
                case Types.OTHER -> database.valueFromString(fieldTypeName, stringItem);
                case Types.ARRAY -> throw new RuntimeException("Unreachable case");
                default -> checkExternal(stringItem);
            };
        }
        return dataItem;
    }

    /**
     * Convert string value to object with type specified by column type.
     *
     * @param stringItem text value
     * @param field      column info
     * @param dataField  field info
     * @return converted value
     */
    protected Object string2object(String stringItem, ColumnInfo field, RequestInfo.Field dataField) {
        try {
            LOG.trace("Convert {} into object", field);
            Object dataItem;
            if (stringItem == null) {
                return null;
            }
            if (Types.ARRAY == field.type()) {
                String[] strElems = StringUtils.stripAll(StringUtils.split(stringItem, settings.csvArrayDelimiter()));
                for (int i = 0; i < strElems.length; i++) {
                    strElems[i] = checkExternal(strElems[i]);
                }
                if (strElems.length == 0) {
                    dataItem = strElems;
                } else {
                    int type = database.getSqlType(field.typeName());
                    if (DBUtils.isStringField(type)) {
                        dataItem = strElems;
                    } else {
                        Object[] elems = new Object[strElems.length];
                        for (int i = 0; i < strElems.length; i++) {
                            elems[i] = string2object(strElems[i], type, field.typeName());
                        }
                        dataItem = settings.connection().createArrayOf(field.typeName(), elems);
                    }
                }
            } else {
                dataItem = string2object(stringItem, field.type(), field.typeName());
            }
            return dataItem;
        } catch (Throwable ex) {
            throw new RuntimeException(
                    "Error on field %s [%s], value [%s] conversion: %s"
                            .formatted(dataField.pos() + 1, dataField.name(), stringItem, ex.getMessage()),
                    ex);
        }
    }

}
