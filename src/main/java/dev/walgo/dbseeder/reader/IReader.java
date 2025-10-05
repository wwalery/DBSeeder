package dev.walgo.dbseeder.reader;

import dev.walgo.dbseeder.data.SeedInfo;
import java.io.IOException;
import java.io.InputStream;

public interface IReader {

    String S_NAME_ID = "id";
    String S_NAME_KEY = "keys";
    String S_NAME_ACTION = "action";
    String S_NAME_REFERENCES = "references";
    String S_TABLE_NAME = "table";
    String S_ADDITIONAL_CONDITION = "condition";
    String S_IGNORE_OMITS = "ignoreOmits";

    /**
     * Read table info from stream.
     * 
     * @param resourceName name of resource
     * @param input        stream for resource
     * @return source data after read
     */
    SeedInfo read(String resourceName, InputStream input) throws IOException;

}
