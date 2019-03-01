package manager.lock;

public class LockRequest {

    private String  database;
    private String  table;
    private int     pk;
    private int     type;

    public LockRequest(String database, String table, int pk, int type) {
        this.database = database;
        this.table = table;
        this.pk = pk;
        this.type = type;
    }

    public LockRequest(String database, int type) {
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

    public int getPk() {
        return pk;
    }

    public void setPk(int pk) {
        this.pk = pk;
    }

    public int getType() {
        return type;
    }

    public void setType(int type) {
        this.type = type;
    }
}
