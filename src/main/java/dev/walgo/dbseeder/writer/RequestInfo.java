package dev.walgo.dbseeder.writer;

import dev.walgo.dbseeder.data.AllowNulls;
import java.util.List;
import org.apache.commons.lang3.builder.ReflectionToStringBuilder;
import org.immutables.value.Value;

@Value.Immutable
@Value.Style(visibility = Value.Style.ImplementationVisibility.PRIVATE, overshadowImplementation = true, jdkOnly = true)
public abstract class RequestInfo {

    public abstract String sql();

    public abstract List<Field> fields();

    @AllowNulls
    public abstract List<String> data();

    public static class Builder extends RequestInfoBuilder {
    }

    public static class Field {

        public final String name;
        public final int pos;

        public Field(String name, int pos) {
            this.name = name;
            this.pos = pos;
        }

        @Override
        public String toString() {
            return ReflectionToStringBuilder.reflectionToString(this);
        }

    }

}
