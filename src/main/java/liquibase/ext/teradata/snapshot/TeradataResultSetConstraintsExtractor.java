package liquibase.ext.teradata.snapshot;

import liquibase.CatalogAndSchema;
import liquibase.database.AbstractJdbcDatabase;
import liquibase.database.Database;
import liquibase.exception.DatabaseException;
import liquibase.snapshot.CachedRow;
import liquibase.snapshot.DatabaseSnapshot;
import liquibase.snapshot.TeradataResultSetCache;
import liquibase.structure.core.Schema;

import java.sql.SQLException;
import java.util.List;

/**
 * Auxiliary class
 */
public class TeradataResultSetConstraintsExtractor extends TeradataResultSetCache.SingleResultSetExtractor {

    private Database database;
    private String catalogName;
    private String schemaName;
    private String tableName;

    public TeradataResultSetConstraintsExtractor(DatabaseSnapshot databaseSnapshot, String catalogName, String schemaName, String tableName) {
        super(databaseSnapshot.getDatabase());
        this.database = databaseSnapshot.getDatabase();
        this.catalogName = catalogName;
        this.schemaName = schemaName;
        this.tableName = tableName;
    }

    public boolean bulkContainsSchema(String schemaKey) {
        return false;
    }

    public TeradataResultSetCache.RowData rowKeyParameters(CachedRow row) {
        return new TeradataResultSetCache.RowData(
            this.catalogName,
            this.schemaName,
            this.database,
            new String[]{row.getString("TABLE_NAME")});
    }

    public TeradataResultSetCache.RowData wantedKeyParameters() {
        return new TeradataResultSetCache.RowData(
            this.catalogName,
            this.schemaName,
            this.database,
            new String[]{tableName});
    }

    public List<CachedRow> fastFetchQuery() throws SQLException, DatabaseException {
        CatalogAndSchema catalogAndSchema = new CatalogAndSchema(catalogName, schemaName).customize(database);
        return this.executeAndExtract(this.createSql(
            ((AbstractJdbcDatabase) database).getJdbcCatalogName(catalogAndSchema),
            ((AbstractJdbcDatabase) database).getJdbcSchemaName(catalogAndSchema),
             tableName),  database, false);
    }

    public List<CachedRow> bulkFetchQuery() throws SQLException, DatabaseException {
        CatalogAndSchema catalogAndSchema = new CatalogAndSchema(catalogName, schemaName).customize(database);
        return this.executeAndExtract(createSql(
            ((AbstractJdbcDatabase) database).getJdbcCatalogName(catalogAndSchema),
            ((AbstractJdbcDatabase) database).getJdbcSchemaName(catalogAndSchema),
            (String) null), database);
    }

    private String createSql(String catalog, String schema, String table) {
        CatalogAndSchema catalogAndSchema = new CatalogAndSchema(catalog, schema).customize(database);
        String jdbcSchemaName = database.correctObjectName(
            ((AbstractJdbcDatabase) database).getJdbcSchemaName(catalogAndSchema), Schema.class);
        String sql = "SELECT IndexName AS CONSTRAINT_NAME, IndexType AS CONSTRAINT_TYPE, TableName AS TABLE_NAME " +
            "FROM DBC.IndicesV WHERE DatabaseName='" + jdbcSchemaName + "' AND IndexType = 'U'";
        if (table != null) {
            sql = sql + " AND TableName='" + table + "'";
        }
        return sql;
    }
}
