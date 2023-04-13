package dev.walgo.dbseeder.reader;

import dev.walgo.dbseeder.data.SeedInfo;
import java.io.IOException;
import java.io.InputStream;

public interface IReader {

    /**
     * Read table info from stream.
     * 
     * @param resourceName name of resource
     * @param input        stream for resource
     * @return source data after read
     */
    SeedInfo read(String resourceName, InputStream input) throws IOException;

}
