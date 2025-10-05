package dev.walgo.dbseeder.reader;

import dev.walgo.dbseeder.DBSSettings;
import dev.walgo.dbseeder.SourceType;
import dev.walgo.dbseeder.reader.csv.CSVReader;
import dev.walgo.dbseeder.reader.json.JSONReader;
import org.apache.commons.io.FilenameUtils;

public class ReaderFactory {

    public static IReader getReader(DBSSettings settings, String fileName) {
        SourceType sourceType = settings.sourceType();
        if ((sourceType == null) || (sourceType == SourceType.ANY)) {
            sourceType = SourceType.fromExtension(FilenameUtils.getExtension(fileName));
        }
        if (sourceType == null) {
            throw new RuntimeException("Unknown reader type for file [%s], original source type: [%s]"
                    .formatted(fileName, settings.sourceType()));
        }
        IReader result = switch (sourceType) {
            case CSV -> new CSVReader(settings);
            case JSON -> new JSONReader(settings);
            default -> throw new RuntimeException("Unknown reader type: [%s]".formatted(settings.sourceType()));
        };
        return result;
    }

}
