package liquibase.ext.teradata.snapshot;

import liquibase.Scope;
import liquibase.database.Database;
import liquibase.exception.DatabaseException;
import liquibase.executor.ExecutorService;
import liquibase.ext.teradata.database.TeradataDatabase;
import liquibase.snapshot.CachedRow;
import liquibase.snapshot.DatabaseSnapshot;
import liquibase.snapshot.SnapshotGenerator;
import liquibase.snapshot.jvm.UniqueConstraintSnapshotGenerator;
import liquibase.statement.core.RawSqlStatement;
import liquibase.structure.DatabaseObject;
import liquibase.structure.core.Relation;
import liquibase.structure.core.Schema;
import liquibase.structure.core.Table;
import liquibase.structure.core.UniqueConstraint;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;

public class TeradataUniqueConstraintSnapshotGenerator extends UniqueConstraintSnapshotGenerator {

    public TeradataUniqueConstraintSnapshotGenerator() {
    }

    @Override
    public int getPriority(Class<? extends DatabaseObject> objectType, Database database) {
        return database instanceof TeradataDatabase ? 5 : -1;
    }

    public Class<? extends SnapshotGenerator>[] replaces() {
        return new Class[]{UniqueConstraintSnapshotGenerator.class};
    }

    protected List<CachedRow> listConstraints(Table table, DatabaseSnapshot snapshot, Schema schema)
        throws DatabaseException, SQLException
    {
        return new TeradataResultSetConstraintsExtractor(
            snapshot,
            schema.getCatalogName(),
            schema.getName(),
            table.getName()).fastFetch();
    }

    protected List<Map<String, ?>> listColumns(UniqueConstraint example, Database database, DatabaseSnapshot snapshot)
        throws DatabaseException
    {
        Relation table = example.getRelation();
        Schema schema = table.getSchema();
        String name = example.getName();
        String schemaName = database.correctObjectName(schema.getName(), Schema.class);
        String constraintName = database.correctObjectName(name, UniqueConstraint.class);
        String tableName = database.correctObjectName(table.getName(), Table.class);
        String sql = "SELECT IndexName AS CONSTRAINT_NAME, ColumnName as COLUMN_NAME FROM DBC.IndicesV " +
            "WHERE IndexType = 'U'";
        if (schemaName != null) {
            sql = sql + " AND DatabaseName='" + schemaName + "' ";
        }

        if (tableName != null) {
            sql = sql + "and TableName='" + tableName + "' ";
        }

        if (constraintName != null) {
            sql = sql + "and IndexName='" + constraintName + "'";
        }

        return (Scope.getCurrentScope().getSingleton(ExecutorService.class))
            .getExecutor("jdbc", database).queryForList(new RawSqlStatement(sql));
    }
}
