package manager.lock;

public class Lock {

    private String  database;
    private String  table;
    private Integer pk;
    private int     type;

    public Lock(String database, String table, int pk, int type) {
        this.database = database;
        this.table = table;
        this.pk = pk;
        this.type = type;
    }

    public Lock(String database, int type) {
        this.database = database;
        this.type = type;
    }

    public String getDatabase() {
        return database;
    }

    public void setDatabase(String database) {
        this.database = database;
    }

    public String getTable() {
        return table;
    }

    public void setTable(String table) {
        this.table = table;
    }

    public Integer getPk() {
        return pk;
    }

    public void setPk(Integer pk) {
        this.pk = pk;
    }

    public int getType() {
        return type;
    }

    public void setType(int type) {
        this.type = type;
    }
}
