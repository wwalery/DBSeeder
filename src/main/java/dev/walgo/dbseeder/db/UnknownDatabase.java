package dev.walgo.dbseeder.db;

import java.sql.Connection;
import java.sql.Types;

public class UnknownDatabase implements Database {

    @Override
    public void setConnecton(Connection conn) {
        // do nothing
    }

    @Override
    public boolean insertHasReturning() {
        return false;
    }

    @Override
    public boolean handlesJDBCUrl(String url) {
        return false;
    }

    @Override
    public int getSqlType(String typeName) {
        return Types.VARCHAR;
    }

    @Override
    public Object valueFromString(String typeName, String value) {
        return value;
    }

}
