package base;

public class SimpleGeneratedTable {

    private String tableName;
    private String alias;

    public SimpleGeneratedTable(String tableName, String alias) {
        this.tableName = tableName;
        this.alias = alias;
    }

    public String getTableName() {
        return tableName;
    }

    public String getAlias() {
        return alias;
    }

    public String toString() {
        return String.format("%s|%s", tableName, alias);
    }
}
