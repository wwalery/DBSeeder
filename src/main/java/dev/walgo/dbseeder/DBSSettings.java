package dev.walgo.dbseeder;

import dev.walgo.dbseeder.data.DataRow;
import dev.walgo.dbseeder.data.SeedInfo;
import java.sql.Connection;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import javax.annotation.Nullable;
import org.apache.commons.lang3.function.TriConsumer;
import org.immutables.value.Value;
import org.immutables.value.Value.Style.ImplementationVisibility;

/**
 * There are a few listeners for data processing:
 * <ul>
 * <li><b>onStartData</b> - called before table processing, before all checking</li>
 * <li><b>onRow</b> - called before processing for every row, before all checking</li>
 * <li><b>onInsert</b> - called before each insert</li>
 * <li><b>onUpdate</b> - called before each update</li>
 * <li><b>onAfterInsert</b> - called after each insert. Used only for DBs with INSERT that support the RETURNING clause.
 * Currently only PostgreSQL is supported</li>
 * </ul>
 **/
@Value.Immutable
@Value.Style(visibility = ImplementationVisibility.PRIVATE, overshadowImplementation = true, jdkOnly = true)
public abstract class DBSSettings {

    public static final String ANY_TABLE = "*";

    public abstract Connection connection();

    public abstract String sourceDir();

    /**
     * Source files extension.
     * <p>
     * Obtained from source type when not set up exactly
     *
     * @return extension
     */
    @Value.Default
    public String sourceExt() {
        return sourceType().getExtension();
    }

    public abstract String dbSchema();

    @Value.Default
    public SourceType sourceType() {
        return SourceType.CSV;
    }

    @Value.Default
    public char csvArrayDelimiter() {
        return '|';
    }

    @Value.Default
    public char csvDelimiter() {
        return ';';
    }

    @Value.Default
    public String csvComment() {
        return "#";
    }

    @Value.Default
    public String csvMultiRefDelimiter() {
        return "##";
    }

    @Value.Default
    public String externalValueRef() {
        return "@@";
    }

    @Nullable
    @Value.Default
    public ClassLoader classLoader() {
        return null;
    }

    public abstract Map<String, Consumer<SeedInfo>> onStartData();

    /**
     * called before processing for every row, before all checking.
     **/
    public abstract Map<String, BiConsumer<SeedInfo, DataRow>> onRow();

    /**
     * called before each insert.
     **/
    public abstract Map<String, BiConsumer<SeedInfo, DataRow>> onInsert();

    /**
     * called before each update.
     **/
    public abstract Map<String, BiConsumer<SeedInfo, DataRow>> onUpdate();

    /**
     * Called after all table processing, after all checking.
     **/
    public abstract Map<String, Consumer<SeedInfo>> onEndData();

    /**
     * called after each insert. Used only for DBs with INSERT that support the RETURNING clause. Currently only
     * PostgreSQL is supported.
     **/
    public abstract Map<String, TriConsumer<SeedInfo, DataRow, Map<String, Object>>> onAfterInsert();

    public static class Builder extends DBSSettingsBuilder {
    }

}
