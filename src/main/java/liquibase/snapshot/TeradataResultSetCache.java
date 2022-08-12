package liquibase.snapshot;

import liquibase.database.Database;
import liquibase.exception.DatabaseException;

import java.sql.SQLException;
import java.util.List;

public class TeradataResultSetCache extends ResultSetCache {

    public abstract static class SingleResultSetExtractor extends liquibase.snapshot.ResultSetCache.SingleResultSetExtractor {
        public SingleResultSetExtractor(Database database) {
            super(database);
        }

        protected boolean shouldBulkSelect(String schemaKey, TeradataResultSetCache resultSetCache) {
            return super.shouldBulkSelect(schemaKey, resultSetCache);
        }

        public List<CachedRow> executeAndExtract(String sql, Database database) throws DatabaseException, SQLException {
            return super.executeAndExtract(sql, database);
        }

        public List<CachedRow> executeAndExtract(String sql, Database database, boolean informixTrimHint) throws DatabaseException, SQLException {
            return super.executeAndExtract(sql, database, informixTrimHint);
        }
    }

    public static class RowData extends liquibase.snapshot.ResultSetCache.RowData {
        public RowData(String catalog, String schema, Database database, String... parameters) {
            super(catalog, schema, database, parameters);
        }
    }
}
