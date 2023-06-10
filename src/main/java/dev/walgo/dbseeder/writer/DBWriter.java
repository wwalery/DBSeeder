package dev.walgo.dbseeder.writer;

import dev.walgo.dbseeder.DBSSettings;
import dev.walgo.dbseeder.data.ActionType;
import dev.walgo.dbseeder.data.DataRow;
import dev.walgo.dbseeder.data.ReferenceInfo;
import dev.walgo.dbseeder.data.SeedInfo;
import dev.walgo.walib.db.ColumnInfo;
import dev.walgo.walib.db.DBInfo;
import dev.walgo.walib.db.TableInfo;
import java.math.BigDecimal;
import java.math.BigInteger;
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
import java.util.Map;
import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.handlers.ScalarHandler;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DBWriter implements IWriter {

    private static final Logger LOG = LoggerFactory.getLogger(DBWriter.class);
    private static final QueryRunner SQL = new QueryRunner();

    private final List<SeedInfo> infos;
//    private final String schema;
    private final DBSSettings settings;
    private final DBInfo dbInfo;

    public DBWriter(List<SeedInfo> infos, DBSSettings settings) {
        this.infos = infos;
//        this.schema = schema;
        this.settings = settings;
        this.dbInfo = new DBInfo(settings.connection(), null, settings.dbSchema(), null);
    }

    private void checkSeed(SeedInfo info) {
        try {
            Map<String, TableInfo> tableMap = dbInfo.getTablesAsMap();
            TableInfo table = tableMap.get(info.getTableName().toLowerCase());
            if (table == null) {
                throw new RuntimeException("Table [%s] doesn't exist".formatted(info.getTableName()));
            }
            Map<String, ColumnInfo> fields = table.getFields();
            for (int i = 0; i < info.getFields().size(); i++) {
                String dataField = info.getFields().get(i);
                String fieldName = dataField.toLowerCase();
                ColumnInfo field = fields.get(fieldName);
                if (field == null) {
                    throw new RuntimeException(
                            "Field N %s [%s] doesn't exists in table [%s]".formatted(i + 1, dataField,
                                    info.getTableName()));
                }
                if (info.getReferences().containsKey(fieldName)) {
                    ReferenceInfo ref = info.getReferences().get(fieldName);
                    TableInfo refTable = tableMap.get(ref.getTableName().toLowerCase());
                    if (refTable == null) {
                        throw new RuntimeException("Reference table [%s] doesn't exist".formatted(ref.getTableName()));
                    }
                    field = refTable.getFields().get(ref.getTableColumn().toLowerCase());
                    if (field == null) {
                        throw new RuntimeException(
                                "Field [%s] doesn't exists in reference table [%s]".formatted(ref.getTableColumn(),
                                        ref.getTableName()));
                    }
                    if (refTable.getKeys().isEmpty()) {
                        throw new RuntimeException(
                                "Referenced table [%s] hasn't key columns".formatted(ref.getTableName()));
                    }
                    if (refTable.getKeys().size() > 1) {
                        throw new RuntimeException(
                                "Referenced table [%s] has more than one key columns".formatted(ref.getTableName()));
                    }
                    ref.setTableKeyColumn(refTable.getKeys().get(0));
                }
            }
            int i = 1;
            for (String dataField : info.getKeys().keySet()) {
                String fieldName = dataField.toLowerCase();
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

    @Override
    public Pair<Integer, Integer> write(SeedInfo info) {
        LOG.info("Process table: [{}], resource: [{}]", info.getTableName(), info.getResourceName());
        checkSeed(info);
        try {
            switch (info.getAction()) {
                case IGNORE:
                    return Pair.of(0, 0);
                case IGNORE_NOT_EMPTY:
                    Number count = SQL.query(settings.connection(),
                            "SELECT COUNT(*) FROM %s".formatted(info.getTableName()),
                            new ScalarHandler<>());
                    if (count.intValue() > 0) {
                        return Pair.of(0, 0);
                    }
                    break;
                default:
                    // do nothing
            }
            SQLGenerator generator = new SQLGenerator(infos);
            int inserted = 0;
            int updated = 0;
            for (DataRow data : info.getData()) {
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
                                RequestInfo insertData = generator.insert(info, data);
                                update(info, insertData);
                                inserted++;
                            }
                            break;
                        case MODIFY:
                            if (!recordExists) {
                                RequestInfo insertData = generator.insert(info, data);
                                update(info, insertData);
                                inserted++;
                            } else {
                                RequestInfo updateData = generator.update(info, data);
                                updated += update(info, updateData);
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
            TableInfo table = tableMap.get(info.getTableName().toLowerCase());
//            if (table == null) {
//                throw new RuntimeException("Table [%s] doesn't exist".formatted(info.getTableName()));
//            }
            Map<String, ColumnInfo> fields = table.getFields();
            for (int i = 0; i < requestData.fields().size(); i++) {
                RequestInfo.Field dataField = requestData.fields().get(i);
                String fieldName = dataField.name.toLowerCase();
                ColumnInfo field = fields.get(fieldName);
//                if (field == null) {
//                    throw new RuntimeException(
//                            "Field [%s] doesn't exists in table [%s]".formatted(dataField, info.getTableName()));
//                }
                String stringItem = requestData.data().get(i);
                if (info.getReferences().containsKey(fieldName)) {
                    ReferenceInfo ref = info.getReferences().get(fieldName);
                    TableInfo refTable = tableMap.get(ref.getTableName().toLowerCase());
//                    if (refTable == null) {
//                        throw new RuntimeException("Reference table [%s] doesn't exist".formatted(ref.getTableName()));
//                    }
                    field = refTable.getFields().get(ref.getTableColumn().toLowerCase());
//                    if (field == null) {
//                        throw new RuntimeException(
//                                "Field [%s] doen't exists in reference table [%s]".formatted(ref.getTableColumn(),
//                                        ref.getTableName()));
//                    }
                }
                try {
                    Object dataItem = string2object(stringItem, field);
                    data.add(dataItem);
                } catch (Throwable ex) {
                    throw new RuntimeException(
                            "Error on field %s [%s], value [%s] conversion: %s"
                                    .formatted(dataField.pos + 1, dataField.name, stringItem, ex.getMessage()),
                            ex);
                }
            }
            return data.toArray(Object[]::new);
        } catch (SQLException ex) {
            throw new RuntimeException(ex);
        }

    }

    /**
     * Convert string value to object with type specified by column type.
     *
     * @param stringItem text value
     * @param field      column info
     * @return converted value
     */
    protected Object string2object(String stringItem, ColumnInfo field) {
        LOG.trace("Convert {} into object", field);
        Object dataItem;
        if (stringItem == null) {
            dataItem = null;
        } else if (field.isString()) {
            dataItem = stringItem;
        } else {
            dataItem = switch (field.type()) {
                case Types.SMALLINT ->
                    Integer.valueOf(stringItem);
                case Types.BIGINT ->
                    Long.valueOf(stringItem);
                case Types.BOOLEAN,
                        Types.BIT ->
                    Boolean.valueOf(stringItem);
                case Types.DATE ->
                    Date.valueOf(stringItem);
                case Types.DECIMAL, Types.DOUBLE, Types.FLOAT, Types.NUMERIC, Types.REAL, Types.TINYINT ->
                    new BigDecimal(stringItem);
                case Types.INTEGER ->
                    Integer.valueOf(stringItem);
                case Types.TIME ->
                    Time.valueOf(stringItem);
                case Types.TIMESTAMP ->
                    stringItem.contains("T") ? Timestamp.from(Instant.parse(stringItem))
                            : Timestamp.valueOf(stringItem);
                case Types.TIMESTAMP_WITH_TIMEZONE ->
                    ZonedDateTime.parse(stringItem);
                case Types.TIME_WITH_TIMEZONE ->
                    ZonedDateTime.parse(stringItem);
                case Types.BINARY, Types.VARBINARY ->
                    new BigInteger(stringItem, 2).toByteArray();
                case Types.ARRAY -> {
                    String[] elems = StringUtils.stripAll(StringUtils.split(stringItem, settings.csvArrayDelimiter()));
                    yield elems;
                }
                default ->
                    stringItem;
            };
        }
        return dataItem;
    }

}
