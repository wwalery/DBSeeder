package dev.walgo.dbseeder.reader;

import dev.walgo.dbseeder.SourceType;
import dev.walgo.dbseeder.reader.csv.CSVReader;

public class ReaderFactory {

    public static IReader getReader(SourceType readerType) {
        IReader result = switch (readerType) {
            case CSV -> new CSVReader();
            default -> throw new RuntimeException("Unknown reader type: [%s]".formatted(readerType));
        };
        return result;
    }

}
