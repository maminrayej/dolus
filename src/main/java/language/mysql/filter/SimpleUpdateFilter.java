package language.mysql.filter;

import base.SimpleUpdateContainer;
import config.ConfigUtilities;
import config.MongoDBConfigContainer;
import config.MySqlConfigContainer;
import config.StorageConfigContainer;

import java.util.HashMap;

public class SimpleUpdateFilter {

    private SimpleUpdateContainer simpleUpdateContainer;

    private HashMap<String,String> mySqlKeyValuePairs;
    private MySqlConfigContainer mySqlConfigContainer;

    private HashMap<String,String> mongoKeyValuePairs;
    private MongoDBConfigContainer mongoDBConfigContainer;

    private String primaryKey;

    public SimpleUpdateFilter(SimpleUpdateContainer simpleUpdateContainer) {
        this.simpleUpdateContainer = simpleUpdateContainer;

        mySqlKeyValuePairs = new HashMap<>();
        mongoKeyValuePairs = new HashMap<>();
    }

    public void filterKeyValuePairs() {

        HashMap<String,String> keyValuePairs = simpleUpdateContainer.getKeyValuePairs();

        String tableName = simpleUpdateContainer.getTableName();

        keyValuePairs.keySet().forEach( key -> {
            String attribute = getAttribute(key);

            StorageConfigContainer foundStorage = ConfigUtilities.findStorage(tableName,attribute);

            if (foundStorage instanceof MySqlConfigContainer) {
                mySqlConfigContainer = (MySqlConfigContainer) foundStorage;

                mySqlKeyValuePairs.put(key, keyValuePairs.get(key));
            }
            else if (foundStorage instanceof MongoDBConfigContainer) {
                mongoDBConfigContainer = (MongoDBConfigContainer) foundStorage;

                mongoKeyValuePairs.put(key, keyValuePairs.get(key));
            }
        });

        if (mySqlConfigContainer != null)
            primaryKey = mySqlConfigContainer.getPrimaryKey(tableName);
        else
            primaryKey = mongoDBConfigContainer.getPrimaryKey(tableName);
    }

    public HashMap<String, String> getMySqlKeyValuePairs() {
        return mySqlKeyValuePairs;
    }

    public HashMap<String, String> getMongoKeyValuePairs() {
        return mongoKeyValuePairs;
    }

    public String getPrimaryKey() {
        return primaryKey;
    }

    private String getAttribute(String key) {
        return key.split("\\.")[1];
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
