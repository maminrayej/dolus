package language.mysql.translator;

import base.SimpleUpdateContainer;
import language.mysql.filter.SimpleUpdateFilter;

import java.util.HashMap;
import java.util.LinkedList;

public class SimpleUpdateTranslator {

    private SimpleUpdateContainer   simpleUpdateContainer;
    private SimpleUpdateFilter      simpleUpdateFilter;

    private String mySqlUpdateStatement;
    private String mongoUpdateStatement;

    public SimpleUpdateTranslator(SimpleUpdateContainer simpleUpdateContainer, SimpleUpdateFilter simpleUpdateFilter) {
        this.simpleUpdateContainer = simpleUpdateContainer;
        this.simpleUpdateFilter = simpleUpdateFilter;
    }

    public String getFirstPhaseSelect() {
        String tableName = simpleUpdateContainer.getTableName();
        String alias     = simpleUpdateContainer.getAlias();
        String primaryKey = simpleUpdateFilter.getPrimaryKey();
        String whereClause = simpleUpdateContainer.getWhereClause();

        return ("SELECT " + alias + "." + primaryKey + " FROM " + tableName + " " + alias +" WHERE " + whereClause).toUpperCase();
    }

    public void translateUpdateStatements(LinkedList<Integer> ids) {

        StringBuilder mySqlStatementBuffer = new StringBuilder();
        StringBuilder mongoStatementBuffer = new StringBuilder();

        String tableName = simpleUpdateContainer.getTableName();
        String alias = simpleUpdateContainer.getAlias();
        String primaryKey = simpleUpdateFilter.getPrimaryKey();

        if (simpleUpdateFilter.getMySqlKeyValuePairs().isEmpty())
            mySqlUpdateStatement = null;
        else {
            mySqlStatementBuffer.append("UPDATE ").append(tableName).append(" ").append(alias).append(" SET ");
            HashMap<String,String> mySqlKeyValuePairs = simpleUpdateFilter.getMySqlKeyValuePairs();
            Object[] keys = mySqlKeyValuePairs.keySet().toArray();
            for (int i = 0; i < keys.length; i++) {
                if (i != 0)
                    mySqlStatementBuffer.append(", ");

                mySqlStatementBuffer.append(keys[i]).append(" = ").append(mySqlKeyValuePairs.get(keys[i]));
            }

            mySqlStatementBuffer.append(" WHERE ").append(alias).append(".").append(primaryKey).append(" IN (");
            Object[] idArray = ids.toArray();
            for (int i = 0; i < idArray.length; i++) {
                if (i != 0)
                    mySqlStatementBuffer.append(", ");

                mySqlStatementBuffer.append(idArray[i]);
            }
            mySqlStatementBuffer.append(")");

            mySqlUpdateStatement = mySqlStatementBuffer.toString();
        }
        if (simpleUpdateFilter.getMongoKeyValuePairs().isEmpty())
            mongoUpdateStatement = null;
        else {
            mongoStatementBuffer.append("db.").append(tableName).append(".").append("update").append("( {");

            mongoStatementBuffer.append(primaryKey).append(": ").append("{ $in: [ ");

            Object[] idArray = ids.toArray();
            for (int i = 0; i < idArray.length; i++) {
                if (i != 0)
                    mongoStatementBuffer.append(", ");

                mongoStatementBuffer.append(idArray[i]);
            }
            mongoStatementBuffer.append("] } }, {");


            HashMap<String,String> mongoKeyValuePairs = simpleUpdateFilter.getMongoKeyValuePairs();
            Object[] keys = mongoKeyValuePairs.keySet().toArray();
            for (int i = 0; i < keys.length; i++) {
                if (i != 0)
                    mongoStatementBuffer.append(", ");

                mongoStatementBuffer.append(keys[i]).append(":").append(mongoKeyValuePairs.get(keys[i]));
            }
            mongoStatementBuffer.append(" } )");

            mongoUpdateStatement = mongoStatementBuffer.toString();
        }
    }

    public String getMySqlUpdateStatement() {
        return mySqlUpdateStatement;
    }

    public String getMongoUpdateStatement() {
        return mongoUpdateStatement;
    }
}
