package dev.walgo.dbseeder;

import dev.walgo.dbseeder.data.SeedInfo;
import dev.walgo.dbseeder.reader.IReader;
import dev.walgo.dbseeder.reader.ReaderFactory;
import dev.walgo.dbseeder.writer.DBWriter;
import dev.walgo.dbseeder.writer.IWriter;
import dev.walgo.walib.ResourceUtils;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Seed DB from file(s).
 */
public class DBSeeder {

    private static final Logger LOG = LoggerFactory.getLogger(DBSeeder.class);

    final List<SeedInfo> infos = new ArrayList<>();

    private final DBSSettings settings;

    public DBSeeder(DBSSettings settings) {
        this.settings = settings;
    }

    /**
     * Read seeder data from file.
     *
     * @param fileName seed file name
     * @return seed data
     */
    public SeedInfo read(String fileName) {
        try (InputStream stream = Files.newInputStream(new File(fileName).toPath())) {
            return read(fileName, stream);
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }

    public SeedInfo read(String fileName, InputStream stream) {
        IReader reader = ReaderFactory.getReader(settings.sourceType());
        try {
            SeedInfo info = reader.read(fileName, stream);
            info.setResourceName(fileName);
            return info;
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }

    /**
     * Read all source data from directory.
     *
     */
    public void read() {
        String fileRegex = StringUtils.replace(settings.sourceDir(), "/", "\\/")
                + ".+?\\"
                + settings.sourceType().getExtension();
        List<String> files = ResourceUtils.findResourceFiles(fileRegex)
                .stream()
                .sorted()
                .toList();
        for (String file : files) {
            LOG.info("Read resource [{}]", file);
            String[] nameParts = StringUtils.split(file, File.separatorChar);
            String fileName = nameParts[nameParts.length - 1];
            InputStream stream = getClass().getClassLoader().getResourceAsStream(file);
            SeedInfo info = read(fileName, stream);
            infos.add(info);
        }
    }

    /**
     * Write single resource data into database.
     *
     * @param info source info
     */
    public void write(SeedInfo info) {
        IWriter writer = new DBWriter(infos, settings);
        writer.write(info);
    }

    /**
     * Write all data into database.
     */
    public void write() {
        for (SeedInfo info : infos) {
            LOG.info("Write table [{}] from resource [{}]", info.getTableName(), info.getResourceName());
            write(info);
        }
    }

}
