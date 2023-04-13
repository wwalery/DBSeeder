package dev.walgo.dbseeder.data;

import java.util.List;
import org.immutables.value.Value;

@Value.Immutable
@Value.Style(visibility = Value.Style.ImplementationVisibility.PRIVATE, overshadowImplementation = true, jdkOnly = true)
public abstract class DataRow {

    public abstract int sourceNumber();

    @AllowNulls
    public abstract List<String> values();

    public static class Builder extends DataRowBuilder {
    }

}
