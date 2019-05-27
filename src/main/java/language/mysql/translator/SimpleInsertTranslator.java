package language.mysql.translator;

import base.SimpleInsertContainer;
import config.MongoDBConfigContainer;
import config.MySqlConfigContainer;

import java.util.HashMap;

public class SimpleInsertTranslator {

    private String tableName;

    private HashMap<String,String> mySqlKeyValuePairs;

    private HashMap<String,String> mongoKeyValuePairs;

    private String mySqlInsertStatement;
    private String mongoDBInsertStatement;

    public SimpleInsertTranslator(String tableName, HashMap<String,String> mySqlKeyValuePairs, HashMap<String,String> mongoKeyValuePairs) {

        this.tableName = tableName;

        this.mySqlKeyValuePairs = mySqlKeyValuePairs;

        this.mongoKeyValuePairs = mongoKeyValuePairs;
    }

    public void translate() {

        StringBuilder mySqlInsertStatementBuffer = new StringBuilder();
        StringBuilder mongoDBInsertStatementBuffer = new StringBuilder();

        //generate mysql insert statement
        if (mySqlKeyValuePairs.size() != 0) {

            mySqlInsertStatementBuffer.append("INSERT INTO ").append(tableName).append("(");

            Object[] mySqlKeyArray = mySqlKeyValuePairs.keySet().toArray();

            //append keys
            for (int i = 0; i < mySqlKeyArray.length; i++) {
                if (i != 0)
                    mySqlInsertStatementBuffer.append(", ");

                mySqlInsertStatementBuffer.append(mySqlKeyArray[i]);
            }
            mySqlInsertStatementBuffer.append(" )");

            mySqlInsertStatementBuffer.append(" VALUES(");
            //append values
            for (int i = 0; i < mySqlKeyArray.length; i++) {
                if (i != 0)
                    mySqlInsertStatementBuffer.append(", ");

                mySqlInsertStatementBuffer.append(mySqlKeyValuePairs.get(mySqlKeyArray[i]));
            }
            mySqlInsertStatementBuffer.append(" )");

            this.mySqlInsertStatement = mySqlInsertStatementBuffer.toString();
        }

        //generate mongodb insert statement
        if (mongoKeyValuePairs.size() != 0) {
            mongoDBInsertStatementBuffer.append("db.").append(tableName).append(".").append("insert( { ");

            Object[] mongoKeyArray = mongoKeyValuePairs.keySet().toArray();

            //append keys
            for (int i = 0; i < mongoKeyArray.length; i++) {
                if (i != 0)
                    mongoDBInsertStatementBuffer.append(", ");

                mongoDBInsertStatementBuffer.append(mongoKeyArray[i]).append(":").append(mongoKeyValuePairs.get(mongoKeyArray[i]));
            }

            mongoDBInsertStatementBuffer.append(" } )");

            this.mongoDBInsertStatement = mongoDBInsertStatementBuffer.toString();
        }
    }

    @Override
    public String toString() {
        return String.format("MySQL insert command: %s\nMongoDB insert command: %s", this.mySqlInsertStatement, this.mongoDBInsertStatement);
    }

    public String getMySqlInsertStatement() {
        return mySqlInsertStatement;
    }

    public String getMongoDBInsertStatement() {
        return mongoDBInsertStatement;
    }
}
