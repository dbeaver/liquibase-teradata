package liquibase.ext.teradata.snapshot;

import liquibase.CatalogAndSchema;
import liquibase.database.AbstractJdbcDatabase;
import liquibase.database.Database;
import liquibase.diff.compare.DatabaseObjectComparatorFactory;
import liquibase.exception.DatabaseException;
import liquibase.ext.teradata.database.TeradataDatabase;
import liquibase.snapshot.CachedRow;
import liquibase.snapshot.DatabaseSnapshot;
import liquibase.snapshot.JdbcDatabaseSnapshot;
import liquibase.snapshot.SnapshotGenerator;
import liquibase.snapshot.jvm.ForeignKeySnapshotGenerator;
import liquibase.structure.DatabaseObject;
import liquibase.structure.core.*;

import java.util.List;

/**
 * This is kind of copy of ForeignKeySnapshotGenerator only for snapshotObject method.
 * It fails for Teradata, because Teradata driver returns null as Deferred parameter
 * and schema compare does not work. Maybe some day we will not need this class.
 */
public class TeradataForeignKeySnapshotGenerator extends ForeignKeySnapshotGenerator {

    public int getPriority(Class<? extends DatabaseObject> objectType, Database database) {
        return database instanceof TeradataDatabase ? 5 : -1;
    }

    public Class<? extends SnapshotGenerator>[] replaces() {
        return new Class[]{liquibase.snapshot.jvm.ForeignKeySnapshotGenerator.class};
    }

    @Override
    protected DatabaseObject snapshotObject(DatabaseObject example, DatabaseSnapshot snapshot) throws DatabaseException {

        Database database = snapshot.getDatabase();

        List<CachedRow> importedKeyMetadataResultSet;
        try {
            Table fkTable = ((ForeignKey) example).getForeignKeyTable();
            String searchCatalog = ((AbstractJdbcDatabase) database).getJdbcCatalogName(fkTable.getSchema());
            String searchSchema = ((AbstractJdbcDatabase) database).getJdbcSchemaName(fkTable.getSchema());
            String searchTableName = database.correctObjectName(fkTable.getName(), Table.class);

            importedKeyMetadataResultSet = ((JdbcDatabaseSnapshot) snapshot).getMetaDataFromCache().getForeignKeys(
                searchCatalog, searchSchema, searchTableName, example.getName());
            ForeignKey foreignKey = null;
            for (CachedRow row : importedKeyMetadataResultSet) {
                String fk_name = cleanNameFromDatabase(row.getString("FK_NAME"), database);
                if (snapshot.getDatabase().isCaseSensitive()) {
                    if (!fk_name.equals(example.getName())) {
                        continue;
                    } else if (!fk_name.equalsIgnoreCase(example.getName())) {
                        continue;
                    }
                }

                if (foreignKey == null) {
                    foreignKey = new ForeignKey();
                }

                foreignKey.setName(fk_name);

                String fkTableCatalog = cleanNameFromDatabase(row.getString(METADATA_FKTABLE_CAT), database);
                String fkTableSchema = cleanNameFromDatabase(row.getString(METADATA_FKTABLE_SCHEM), database);
                String fkTableName = cleanNameFromDatabase(row.getString(METADATA_FKTABLE_NAME), database);
                Table foreignKeyTable = new Table().setName(fkTableName);
                foreignKeyTable.setSchema(new Schema(new Catalog(fkTableCatalog), fkTableSchema));

                foreignKey.setForeignKeyTable(foreignKeyTable);
                Column fkColumn = new Column(cleanNameFromDatabase(row.getString(METADATA_FKCOLUMN_NAME), database)).setRelation(foreignKeyTable);
                boolean alreadyAdded = false;
                for (Column existing : foreignKey.getForeignKeyColumns()) {
                    if (DatabaseObjectComparatorFactory.getInstance()
                        .isSameObject(existing, fkColumn, snapshot.getSchemaComparisons(), database))
                    {
                        alreadyAdded = true; //already added. One is probably an alias
                        break;
                    }
                }
                if (alreadyAdded) {
                    break;
                }

                CatalogAndSchema pkTableSchema = ((AbstractJdbcDatabase) database).getSchemaFromJdbcInfo(
                    row.getString(METADATA_PKTABLE_CAT), row.getString(METADATA_PKTABLE_SCHEM));
                Table tempPkTable = (Table) new Table().setName(row.getString(METADATA_PKTABLE_NAME)).setSchema(
                    new Schema(pkTableSchema.getCatalogName(), pkTableSchema.getSchemaName()));
                foreignKey.setPrimaryKeyTable(tempPkTable);
                Column pkColumn = new Column(cleanNameFromDatabase(row.getString(METADATA_PKCOLUMN_NAME), database))
                    .setRelation(tempPkTable);

                foreignKey.addForeignKeyColumn(fkColumn);
                foreignKey.addPrimaryKeyColumn(pkColumn);

                ForeignKeyConstraintType updateRule = convertToForeignKeyConstraintType(
                    row.getInt(METADATA_UPDATE_RULE), database);
                foreignKey.setUpdateRule(updateRule);

                ForeignKeyConstraintType deleteRule = convertToForeignKeyConstraintType(
                    row.getInt(METADATA_DELETE_RULE), database);
                foreignKey.setDeleteRule(deleteRule);

                foreignKey.setDeferrable(false);
                foreignKey.setInitiallyDeferred(false);

                Index exampleIndex = new Index().setRelation(foreignKey.getForeignKeyTable());
                exampleIndex.getColumns().addAll(foreignKey.getForeignKeyColumns());
                exampleIndex.addAssociatedWith(Index.MARK_FOREIGN_KEY);
                foreignKey.setBackingIndex(exampleIndex);

            }
            if (snapshot.get(ForeignKey.class).contains(foreignKey)) {
                return null;
            }
            return foreignKey;
        } catch (Exception e) {
            throw new DatabaseException(e);
        }
    }
}
