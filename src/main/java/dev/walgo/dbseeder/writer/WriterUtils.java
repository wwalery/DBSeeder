package dev.walgo.dbseeder.writer;

import dev.walgo.dbseeder.DBSSettings;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WriterUtils {

    private static final Logger LOG = LoggerFactory.getLogger(DBWriter.class);

    public static String checkExternal(DBSSettings settings, String item, boolean isEscapeLF) {
        String extRef = settings.externalValueRef() + "{";
        int extRefStart = item.indexOf(extRef);
        if (extRefStart < 0) {
            return item;
        }
        int extRefEnd = item.indexOf("}", extRefStart + extRef.length());
        if (extRefEnd < 0) {
            throw new RuntimeException("External value reference not closed: [" + item + "]");
        }
        int extRefLen = extRef.length();

        String fileName = item.substring(extRefStart + extRefLen, extRefEnd);
        String srcDir = settings.sourceDir().endsWith("/") ? settings.sourceDir() : settings.sourceDir() + "/";
        File dir = new File(srcDir);
        boolean isExternalResource = dir.exists();
        String value = "";
        if (isExternalResource) {
            Path path = new File(srcDir + fileName).toPath();
            try {
                value = Files.readString(path);
            } catch (IOException ex) {
                throw new RuntimeException(ex);
            }
        } else {
            ClassLoader classLoader = settings.classLoader() != null
                    ? settings.classLoader()
                    : ClassLoader.getSystemClassLoader();
            try (InputStream stream = classLoader.getResourceAsStream(srcDir + fileName)) {
                value = new String(stream.readAllBytes(), StandardCharsets.UTF_8);
            } catch (Exception ex) {
                LOG.error("Error on reading resource: [{}]", srcDir + fileName);
                throw new RuntimeException(ex);
            }
        }
        if (isEscapeLF) {
            value = value.replace("\n", "\\n").replace("\r", "\\r");
        }
        return checkExternal(settings, StringUtils.substring(item, 0, extRefStart), isEscapeLF) + value
                + checkExternal(settings, StringUtils.substring(item, extRefEnd + 1), isEscapeLF);
    }

}
