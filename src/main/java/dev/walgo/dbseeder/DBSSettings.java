package dev.walgo.dbseeder;

import dev.walgo.dbseeder.data.DataRow;
import dev.walgo.dbseeder.data.SeedInfo;
import java.sql.Connection;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import org.immutables.value.Value;
import org.immutables.value.Value.Style.ImplementationVisibility;

@Value.Immutable
@Value.Style(visibility = ImplementationVisibility.PRIVATE, overshadowImplementation = true, jdkOnly = true)
public abstract class DBSSettings {

    public static final String ANY = "*";

    public abstract Connection connection();

    public abstract String sourceDir();

    /**
     * Source files extension.
     *
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

    /**
     * 
     * @return
     */
    public abstract Map<String, Consumer<SeedInfo>> onStartData();

    public abstract Map<String, BiConsumer<SeedInfo, DataRow>> onRow();

    public abstract Map<String, BiConsumer<SeedInfo, DataRow>> onInsert();

    public abstract Map<String, BiConsumer<SeedInfo, DataRow>> onUpdate();

    public abstract Map<String, Consumer<SeedInfo>> onEndData();

    public static class Builder extends DBSSettingsBuilder {
    }

}
