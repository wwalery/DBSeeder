package dev.walgo.dbseeder.db;

import java.sql.Connection;

/**
 * Database custom parameters
 */
public interface Database {

    void setConnecton(Connection conn);

    /**
     * Is given JDBC URL is supported by this database?
     * 
     * @param url JDBC URL
     * @return true if URL supported
     */
    boolean handlesJDBCUrl(String url);

    /**
     * Is RETURNING part is supported in INSERT clause?
     * 
     * @return true if supported
     */
    boolean insertHasReturning();

    /**
     * Get JDBC type from custom field name.
     * 
     * @param typeName
     * @return
     */
    int getSqlType(String typeName);

    Object valueFromString(String typeName, String stringValue);

}
