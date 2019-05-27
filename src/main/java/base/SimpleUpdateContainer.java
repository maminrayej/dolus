package base;

import java.util.HashMap;

public class SimpleUpdateContainer {

    private String tableName;
    private String alias;

    private HashMap<String,String> keyValuePairs;

    private String whereClause;

    public SimpleUpdateContainer() {
        keyValuePairs = new HashMap<>();
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getAlias() {
        return alias;
    }

    public void setAlias(String alias) {
        this.alias = alias;
    }

    public HashMap<String, String> getKeyValuePairs() {
        return keyValuePairs;
    }

    public void setKeyValuePairs(HashMap<String, String> keyValuePairs) {
        this.keyValuePairs = keyValuePairs;
    }

    public String getWhereClause() {
        return whereClause;
    }

    public void setWhereClause(String whereClause) {
        this.whereClause = whereClause;
    }

    public void addKeyValuePair(String key, String value) {
        keyValuePairs.put(key, value);
    }

    @Override
    public String toString() {
        StringBuilder buffer = new StringBuilder();
        buffer.append("Table name: ").append(tableName).append("\n");
        buffer.append("Alias: ").append(alias).append("\n");
        buffer.append("Where: ").append(whereClause).append("\n");

        keyValuePairs.keySet().forEach( key -> {
            buffer.append("key: ").append(key).append(", value: ").append(keyValuePairs.get(key)).append("\n");
        });

        return buffer.toString();
    }
}
