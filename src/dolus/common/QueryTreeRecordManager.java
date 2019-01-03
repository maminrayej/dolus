package dolus.common;

/**
 * Is a record manager that manages the query records as a tree named Query Activation Tree.
 *
 * @author m.amin rayej
 * @version 1.0
 * @since 1.0
 */
public class QueryTreeRecordManager implements RecordManager<String, String, QueryTreeRecord<String,String>> {

    @Override
    public String findSymbol(String key) {
        return null;
    }

    @Override
    public void addRecord(QueryTreeRecord record) {

    }
}
