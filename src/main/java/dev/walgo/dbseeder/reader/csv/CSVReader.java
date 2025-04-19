package dev.walgo.dbseeder.reader.csv;

import dev.walgo.dbseeder.DBSSettings;
import dev.walgo.dbseeder.data.ActionType;
import dev.walgo.dbseeder.data.DataRow;
import dev.walgo.dbseeder.data.ReferenceInfo;
import dev.walgo.dbseeder.data.SeedInfo;
import dev.walgo.dbseeder.reader.IReader;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CSVReader implements IReader {

    private static final Logger LOG = LoggerFactory.getLogger(CSVReader.class);
    private static final Pattern REFERENCE_REGEX = Pattern.compile("(.+?)=(.+?)\\((.+?)\\)");

    private final DBSSettings settings;

    public CSVReader(DBSSettings settings) {
        this.settings = settings;
    }

    @Override
    public SeedInfo read(String resourceName, InputStream input) {
        SeedInfo info = new SeedInfo();
        int lineNum = 0;
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(input, StandardCharsets.UTF_8))) {
            String rawLine = reader.readLine();
            String line = StringUtils.trim(rawLine);
            if (StringUtils.isEmpty(line)) {
                throw new RuntimeException("Line 1 (field list) not defined");
            }
            lineNum++;
            parseFieldList(line, info);
            line = reader.readLine();
            if (StringUtils.isEmpty(line)) {
                throw new RuntimeException("Line 2 (settings) not defined");
            }
            lineNum++;
            parseSettings(line, info);
            List<DataRow> data = info.getData();
            while ((rawLine = reader.readLine()) != null) {
                lineNum++;
                line = StringUtils.trim(rawLine);

                if (StringUtils.isEmpty(line) || line.startsWith(settings.csvComment())) {
                    continue;
                }
                DataRow row = new DataRow(lineNum);
                String[] parts = StringUtils.splitPreserveAllTokens(line, settings.csvDelimiter());
                if (parts.length != info.getFields().size()) {
                    LOG.warn("Parts = {}", Arrays.asList(parts));
                    throw new RuntimeException("There is [%s] columns in header but [%s] in line [%s]"
                            .formatted(info.getFields().size(), parts.length, lineNum));
                }
                for (String part : parts) {
                    row.addValues(StringUtils.trim(part));
                }
                data.add(row);
            }
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
        return info;
    }

    private void parseFieldList(String line, SeedInfo info) {
        String[] parts = StringUtils.split(line, settings.csvDelimiter());
        Map<String, Integer> fields = info.getFields();
        for (int i = 0; i < parts.length; i++) {
            String part = parts[i];
            fields.put(StringUtils.trim(part), i);
        }
    }

    private void parseKeyList(String line, SeedInfo info) {
        String[] parts = StringUtils.split(line, CSVSettings.INTERNAL_DELIMITER);
        Map<String, Integer> keys = info.getKeys();
        Map<String, Integer> fields = info.getFields();
        for (String part : parts) {
            String key = StringUtils.trim(part);
            Integer fieldIdx = fields.getOrDefault(key, -1);
            if (fieldIdx < 0) {
                throw new RuntimeException("Unknown key [%s]".formatted(key));
            }
            keys.put(key, fieldIdx);
        }
    }

    private void parseReferences(String line, SeedInfo info) {
        String[] parts = StringUtils.split(line, CSVSettings.INTERNAL_DELIMITER);
        Map<String, ReferenceInfo> refs = info.getReferences();
        Map<String, Integer> fields = info.getFields();
        for (String part : parts) {
            String reference = StringUtils.trim(part);
            Matcher matcher = REFERENCE_REGEX.matcher(reference);
            if (!matcher.matches()) {
                throw new RuntimeException("Reference [%s] doesn't match reference regex [%s]".formatted(reference,
                        REFERENCE_REGEX.pattern()));
            }
            String fieldName = StringUtils.trim(matcher.group(1));
            String tableName = StringUtils.trim(matcher.group(2));
            String tableField = StringUtils.trim(matcher.group(3));
            int fieldIdx = fields.getOrDefault(fieldName, -1);
            if (fieldIdx < 0) {
                throw new RuntimeException("Unknown column [%s] in reference [%s]".formatted(fieldName, reference));
            }
            ReferenceInfo ref = new ReferenceInfo();
            ref.setFieldName(fieldName);
            ref.setTableName(tableName);
            String[] tableFields = StringUtils.split(tableField, settings.csvMultiRefDelimiter());
            ref.setTableColumn(Arrays.asList(tableFields));
            ref.setFieldIdx(fieldIdx);
            refs.put(fieldName, ref);
        }
    }

    private void parseSettings(String line, SeedInfo info) {
        String[] settingsArray = StringUtils.split(line, this.settings.csvDelimiter());
        for (String setting : settingsArray) {
            String[] parts = StringUtils.split(setting, CSVSettings.S_DELIMITER, 2);
            if (parts.length != 2) {
                throw new RuntimeException("Parameter [%s] doesn't have 2 parts (more or less)".formatted(setting));
            }
            switch (StringUtils.trim(parts[0])) {
                case CSVSettings.S_NAME_ACTION:
                    ActionType action = ActionType.valueOf(StringUtils.trim(parts[1]).toUpperCase(Locale.ROOT));
                    info.setAction(action);
                    break;
                case CSVSettings.S_NAME_KEY:
                    parseKeyList(parts[1], info);
                    break;
                case CSVSettings.S_NAME_REFERENCES:
                    parseReferences(parts[1], info);
                    break;
                case CSVSettings.S_TABLE_NAME:
                    String tableName = StringUtils.trim(parts[1]);
                    info.setTableName(tableName);
                    break;
                case CSVSettings.S_ADDITIONAL_CONDITION:
                    String extraCondition = StringUtils.trim(parts[1]);
                    info.setExtraCondition(extraCondition);
                    break;
                default:
                    LOG.warn("Unknown settings: [{}]", setting);
            }
        }
    }

}
