package dev.walgo.dbseeder.db;

import io.agroal.pool.wrapper.ConnectionWrapper;
import java.sql.Connection;

/**
 * Database custom parameters
 */
public abstract class Database {

    protected Connection conn;

    public void setConnecton(Connection conn) {
        if (conn instanceof ConnectionWrapper agroal) {
            this.conn = agroal.getHandler().rawConnection();
        } else {
            this.conn = conn;
        }
    }

    /**
     * Is given JDBC URL is supported by this database?
     * 
     * @param url JDBC URL
     * @return true if URL supported
     */
    public abstract boolean handlesJDBCUrl(String url);

    /**
     * Is RETURNING part is supported in INSERT clause?
     * 
     * @return true if supported
     */
    public abstract boolean insertHasReturning();

    /**
     * Get JDBC type from custom field name.
     * 
     * @param typeName
     * @return
     */
    public abstract int getSqlType(String typeName);

    public abstract Object valueFromString(String typeName, String stringValue);

}
