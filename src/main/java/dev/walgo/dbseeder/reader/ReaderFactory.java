package dev.walgo.dbseeder.reader;

import dev.walgo.dbseeder.DBSSettings;
import dev.walgo.dbseeder.reader.csv.CSVReader;

public class ReaderFactory {

    public static IReader getReader(DBSSettings settings) {
        IReader result = switch (settings.sourceType()) {
            case CSV -> new CSVReader(settings);
            default -> throw new RuntimeException("Unknown reader type: [%s]".formatted(settings.sourceType()));
        };
        return result;
    }

}
