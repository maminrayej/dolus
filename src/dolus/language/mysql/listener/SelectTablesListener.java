package dolus.language.mysql.listener;

import dolus.base.HashMapBasedQueryTreeRecordManager;
import dolus.language.mysql.utilities.MySqlParser;
import dolus.language.mysql.utilities.MySqlParserBaseListener;

/**
 * This class listens and waits to encounter a "table_name alias" in select query.
 * Then asks the record manager to add the symbol to the symbol table of the active query.
 *
 * @author m.amin rayej
 * @version 1.0
 * @since 1.0
 */
public class SelectTablesListener extends MySqlParserBaseListener {

    /**
     * Record Manager manages the records during traversal of the parse tree
     */
    private HashMapBasedQueryTreeRecordManager recordManager;

    /**
     * Default constructor. Initializes the record manager
     *
     * @since 1.0
     */
    public SelectTablesListener() {

        this.recordManager = new HashMapBasedQueryTreeRecordManager();

    }

    /**
     * When enter a SELECT statement.
     *
     * @since 1.0
     */
    @Override
    public void enterQuerySpecification(MySqlParser.QuerySpecificationContext ctx) {

        //aks the record manager to add a new record
        recordManager.addRecord();

    }

    /**
     * When a SELECT statement ends.
     *
     * @since 1.0
     */
    @Override
    public void exitQuerySpecification(MySqlParser.QuerySpecificationContext ctx) {

        //ask the record manager to activate record of the previous SELECT query
        recordManager.decrementDepth();

    }

    /**
     * When encounter a "table_name alias".
     *
     * @since 1.0
     */
    @Override
    public void enterAtomTableItem(MySqlParser.AtomTableItemContext ctx) {

        //extract the table name and its alias
        String tableName = ctx.tableName().getText();
        String alias = ctx.alias.getText();

        //add the symbol to the active record symbol table
        recordManager.addSymbol(alias, tableName);
    }

    /**
     * When the top level SELECT statement ends.
     *
     * @since 1.0
     */
    @Override
    public void exitSqlStatement(MySqlParser.SqlStatementContext ctx) {

        //debug
        System.out.println(recordManager.toString());

        //reset the record pointer in record manager
        recordManager.resetDepth();

    }
}
