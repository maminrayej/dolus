package base;

import java.util.HashMap;

public class SimpleInsertContainer {

    private HashMap<String,String> keyValuePairs;

    private String tableName;

    public SimpleInsertContainer() {
        keyValuePairs = new HashMap<>();
    }

    public String getValue(String key) {
        return keyValuePairs.get(key);
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public void setKeyValuePairs(HashMap<String, String> keyValuePairs) {
        this.keyValuePairs = keyValuePairs;
    }

    public HashMap<String, String> getKeyValuePairs() {
        return keyValuePairs;
    }

    @Override
    public String toString() {
        StringBuilder buffer = new StringBuilder();

        keyValuePairs.keySet().forEach((key) -> {
            buffer.append(key).append(":").append(keyValuePairs.get(key)).append("\n");
        });

        buffer.append("for table: ").append(tableName);

        return buffer.toString();
    }
}
