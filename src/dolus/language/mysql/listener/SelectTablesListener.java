package dolus.language.mysql.listener;

import dolus.language.mysql.utilities.MySqlParser;
import dolus.language.mysql.utilities.MySqlParserBaseListener;
import dolus.common.QueryTreeRecordManager;

public class SelectTablesListener extends  MySqlParserBaseListener{

    private QueryTreeRecordManager recordManager;

    public SelectTablesListener(){

        this.recordManager = new QueryTreeRecordManager();

    }

    @Override
    public void enterQuerySpecification(MySqlParser.QuerySpecificationContext ctx) {

        recordManager.addRecord();

    }

    @Override
    public void exitQuerySpecification(MySqlParser.QuerySpecificationContext ctx) {

        recordManager.decrementDepth();

    }

    @Override
    public void exitSqlStatement(MySqlParser.SqlStatementContext ctx) {

        System.out.println(recordManager.toString());

    }
}
