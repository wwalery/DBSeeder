package dev.walgo.dbseeder;

import java.sql.Connection;
import org.immutables.value.Value;
import org.immutables.value.Value.Style.ImplementationVisibility;

@Value.Immutable
@Value.Style(visibility = ImplementationVisibility.PRIVATE, overshadowImplementation = true, jdkOnly = true)
public abstract class DBSSettings {

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

    public static class Builder extends DBSSettingsBuilder {
    }

}
