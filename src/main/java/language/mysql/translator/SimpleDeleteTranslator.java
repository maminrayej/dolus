package language.mysql.translator;

import base.SimpleDeleteContainer;
import config.ConfigUtilities;
import config.MongoDBConfigContainer;
import config.MySqlConfigContainer;
import config.StorageConfigContainer;

import java.util.LinkedList;

public class SimpleDeleteTranslator {

    private SimpleDeleteContainer simpleDeleteContainer;

    private String mySqlDeleteStatement;
    private String mongoDeleteStatement;

    public SimpleDeleteTranslator(SimpleDeleteContainer simpleDeleteContainer) {
        this.simpleDeleteContainer = simpleDeleteContainer;
    }

    public String generateFirstPhaseSelect() {
        StorageConfigContainer storageConfigContainer = ConfigUtilities.findStorage(simpleDeleteContainer.getTableName());
        String primaryKey = "";
        if (storageConfigContainer instanceof MySqlConfigContainer) {
            primaryKey = storageConfigContainer.getPrimaryKey(simpleDeleteContainer.getTableName());
        } else if (storageConfigContainer instanceof MongoDBConfigContainer) {
            primaryKey = storageConfigContainer.getPrimaryKey(simpleDeleteContainer.getTableName());
        }

        String tableName = simpleDeleteContainer.getTableName();
        String alias = simpleDeleteContainer.getAlias();
        String whereClause = simpleDeleteContainer.getWhereClause();

        return String.format("SELECT %s.%s FROM %s %s WHERE %s", alias,primaryKey,tableName,alias, whereClause);
    }

    public void translate(int[] ids) {
        StringBuilder mySqlStatementBuffer = new StringBuilder();

        String tableName = simpleDeleteContainer.getTableName();
        StorageConfigContainer storageConfigContainer = ConfigUtilities.findStorage(simpleDeleteContainer.getTableName());
        String primaryKey = "";
        if (storageConfigContainer instanceof MySqlConfigContainer) {
            primaryKey = storageConfigContainer.getPrimaryKey(simpleDeleteContainer.getTableName());
        } else if (storageConfigContainer instanceof MongoDBConfigContainer) {
            primaryKey = storageConfigContainer.getPrimaryKey(simpleDeleteContainer.getTableName());
        }

        mySqlStatementBuffer.append("DELETE FROM ").append(tableName).append(" WHERE ").append(primaryKey).append(" IN ( ");

        for (int i = 0; i < ids.length; i++) {
            if (i != 0)
                mySqlStatementBuffer.append(", ");

            mySqlStatementBuffer.append(ids[i]);
        }
        mySqlStatementBuffer.append(" )");

        System.out.println(mySqlStatementBuffer.toString());

        StringBuilder mongoStatementBuffer = new StringBuilder();
        mongoStatementBuffer.append("db.").append(tableName).append(".").append("deleteMany( {");

        mongoStatementBuffer.append(primaryKey).append(": ").append("{ $in: [ ");

        for (int i = 0; i < ids.length; i++) {
            if (i != 0)
                mongoStatementBuffer.append(", ");

            mongoStatementBuffer.append(ids[i]);
        }
        mongoStatementBuffer.append("] } } )");

        System.out.println(mongoStatementBuffer.toString());

    }
}
