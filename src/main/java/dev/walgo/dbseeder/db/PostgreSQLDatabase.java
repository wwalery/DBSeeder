package dev.walgo.dbseeder.db;

import com.google.auto.service.AutoService;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Types;
import org.postgresql.jdbc.PgConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@AutoService(Database.class)
public class PostgreSQLDatabase extends Database {

    private static final Logger LOG = LoggerFactory.getLogger(PostgreSQLDatabase.class);

    @Override
    public void setConnection(Connection conn) {
        super.setConnection(conn);
        if (!(this.conn instanceof PgConnection)) {
            throw new UnsupportedOperationException(
                    "Connection of type [%s] is not PostgreSQL connection".formatted(conn.getClass()));
        }
    }

    @Override
    public boolean insertHasReturning() {
        return true;
    }

    @Override
    public boolean handlesJDBCUrl(String url) {
        return url.startsWith("jdbc:postgresql:") || url.startsWith("jdbc:p6spy:postgresql:");
    }

    @Override
    public int getSqlType(String typeName) {
        if (typeName == null) {
            return Types.VARCHAR;
        }
        String type = typeName.startsWith("_") ? typeName.substring(1) : typeName;
        try {
            return ((PgConnection) conn).getTypeInfo().getSQLType(type);
        } catch (SQLException ex) {
            LOG.error("Can't extract JDBC type from native type: [{}]", typeName, ex);
            return Types.VARCHAR;
        }
    }

    @Override
    public Object valueFromString(String typeName, String stringValue) {
        return stringValue;
    }

}
