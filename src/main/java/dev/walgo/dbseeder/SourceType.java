package dev.walgo.dbseeder;

import java.util.Arrays;
import java.util.List;

public enum SourceType {
    CSV(List.of("csv")),
    JSON(List.of("json")),
    XML(List.of("xml")),
    ANY(List.of());

    private final List<String> extensions;

    SourceType(List<String> extensions) {
        this.extensions = extensions;
    }

    public List<String> getExtensions() {
        if (this == ANY) {
            return Arrays.stream(SourceType.values())
                    .flatMap(it -> it.extensions.stream())
                    .toList();
        } else {
            return extensions;
        }
    }

    public static SourceType fromExtension(String extension) {
        for (SourceType type : values()) {
            if (type.getExtensions().contains(extension)) {
                return type;
            }
        }
        return null;
    }

}
