package language.mysql.filter;

import base.SimpleInsertContainer;
import config.ConfigUtilities;
import config.MongoDBConfigContainer;
import config.MySqlConfigContainer;
import config.StorageConfigContainer;

import java.util.HashMap;

public class SimpleInsertFilter {

    private SimpleInsertContainer simpleInsertContainer;

    private HashMap<String,String> mySqlKeyValuePairs;
    private MySqlConfigContainer mySqlConfigContainer;

    private HashMap<String,String> mongoKeyValuePairs;
    private MongoDBConfigContainer mongoDBConfigContainer;

    public SimpleInsertFilter(SimpleInsertContainer simpleInsertContainer) {
        this.simpleInsertContainer = simpleInsertContainer;

        this.mySqlKeyValuePairs = new HashMap<>();
        this.mongoKeyValuePairs = new HashMap<>();
    }

    public void filterKeyValues() {

        HashMap<String,String> keyValuePairs = simpleInsertContainer.getKeyValuePairs();
        String tableName = simpleInsertContainer.getTableName();

        StringBuilder primaryKeyValue = new StringBuilder();
        StringBuilder primaryKey = new StringBuilder();

        StringBuilder primaryKeyFound = new StringBuilder("false");

        keyValuePairs.keySet().forEach((key) -> {
            StorageConfigContainer foundStorage = ConfigUtilities.findStorage(tableName,key);

            if (foundStorage instanceof MySqlConfigContainer) {
                mySqlConfigContainer = (MySqlConfigContainer) foundStorage;

                if (primaryKeyFound.toString().equals("false")) {
                    primaryKeyValue.append(mySqlConfigContainer.getPrimaryKey(tableName));
                    primaryKeyFound.delete(0,1);
                }

                if (key.equals(primaryKeyValue.toString()))
                    primaryKey.append(keyValuePairs.get(key));

                mySqlKeyValuePairs.put(key, keyValuePairs.get(key));
            }
            else if (foundStorage instanceof MongoDBConfigContainer) {
                mongoDBConfigContainer = (MongoDBConfigContainer) foundStorage;

                mongoKeyValuePairs.put(key, keyValuePairs.get(key));
            }
        });

        if (mongoKeyValuePairs.size() != 0)
            mongoKeyValuePairs.put(primaryKeyValue.toString(), primaryKey.toString());

    }

    public HashMap<String, String> getMySqlKeyValuePairs() {
        return mySqlKeyValuePairs;
    }

    public MySqlConfigContainer getMySqlConfigContainer() {
        return mySqlConfigContainer;
    }

    public HashMap<String, String> getMongoKeyValuePairs() {
        return mongoKeyValuePairs;
    }

    public MongoDBConfigContainer getMongoDBConfigContainer() {
        return mongoDBConfigContainer;
    }

    @Override
    public String toString() {
        StringBuilder buffer = new StringBuilder();

        buffer.append("MySQL key value pairs: ").append("\n");
        mySqlKeyValuePairs.keySet().forEach( (key) -> buffer.append(key).append(":").append(mySqlKeyValuePairs.get(key)).append("\n"));

        buffer.append("MongoDB key value pairs: ").append("\n");
        mongoKeyValuePairs.keySet().forEach( (key) -> buffer.append(key).append(":").append(mongoKeyValuePairs.get(key)).append("\n"));

        return buffer.toString();
    }
}
