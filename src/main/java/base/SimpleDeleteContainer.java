package base;

public class SimpleDeleteContainer {

    private String tableName;
    private String alias;

    private String whereClause;

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

    public String getWhereClause() {
        return whereClause;
    }

    public void setWhereClause(String whereClause) {
        this.whereClause = whereClause;
    }

    @Override
    public String toString() {
        return String.format("Table Name: %s\nAlias: %s\nWhere Clause: %s\n", tableName, alias, whereClause);
    }
}
