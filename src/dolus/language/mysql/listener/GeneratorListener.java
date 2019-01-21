package dolus.language.mysql.listener;

import dolus.base.HashMapBasedQueryTreeRecordManager;
import dolus.common.Log;
import dolus.config.MongoDBConfig;
import dolus.config.MySqlConfig;
import dolus.language.mysql.utilities.MySqlParser;
import dolus.language.mysql.utilities.MySqlParserBaseListener;

public class GeneratorListener extends MySqlParserBaseListener {

    private static String componentName = "GeneratorListener";

    /**
     * Record Manager manages the records during traversal of the parse tree
     */
    private HashMapBasedQueryTreeRecordManager recordManager;

    /**
     * Contains MySQL meta data and configuration
     */
    private MySqlConfig mySqlConfig;

    /**
     * Contains MongoDb meta data and configuration
     */
    private MongoDBConfig mongoDBConfig;

    /**
     * Indicates whether generating phase was successful or not
     */
    private boolean result;


    private boolean firstSelect =true;

    /**
     * Default constructor. Initializes the record manager
     *
     * @since 1.0
     */
    public GeneratorListener(HashMapBasedQueryTreeRecordManager recordManager, MySqlConfig mySqlConfig, MongoDBConfig mongoDBConfig) {

        this.recordManager = recordManager;
        this.mySqlConfig = mySqlConfig;
        this.mongoDBConfig = mongoDBConfig;

        this.result = true;

    }

    /**
     * When enter a SELECT statement.
     *
     * @since 1.0
     */
    @Override
    public void enterQuerySpecification(MySqlParser.QuerySpecificationContext ctx) {

        //aks the record manager to activate the appropriate child record
        if (!firstSelect)
            recordManager.incrementDepth();
        else
            firstSelect = false;
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
     * When encounters a column name
     */
    @Override
    public void enterFullColumnName(MySqlParser.FullColumnNameContext ctx) {

        //if generating failed, do not generate any more
        if (!result)
            return;

        //extract the alias from alias.attribute
        String alias = ctx.uid().getText();

        //extract dotted attribute ".attribute"
        String dottedAttribute = ctx.dottedId(0).getText();

        //just get the attribute and discard the "."
        String attribute = dottedAttribute.substring(1);

        //find the table name the alias is referring to
        String tableName = recordManager.findSymbol(alias);

        //if alias does not refer to any table, log an error message
        if (tableName == null){
            String logMsg = String.format("Query accessed alias: %s but alias does not refer to any table in the query", alias);
            Log.log(logMsg, componentName, Log.ERROR);
            result = false;
            return;
        }

        //generate a table based on the accessed attribute
        String generatedTableName = generateTableName(tableName, attribute);

        //if generating the table failed, log an error message and stop generating
        if (generatedTableName == null){
            Log.log("Generating phase failed", componentName, Log.ERROR);
            result = false;//generating failed
            return;
        }

        //generate a table alias for the generated table name
        String generatedTableAlias = generatedTableName.replaceAll("\\.","_") + "_" + alias.toLowerCase();

        //add these generated aliases to the appropriate record
        recordManager.addGeneratedAliases(alias, alias + dottedAttribute,
                generatedTableAlias + dottedAttribute.toLowerCase(),
                                tableName, generatedTableAlias);
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

    /**
     * Shows whether generating phase was successful or not
     *
     * @return result of the generating phase
     */
    public boolean getResult() {
        return result;
    }


    /**
     * Generates a table according to the accessed attribute
     *
     * @param tableName name of the table in the original query
     * @param attribute attribute accessed by the alias
     * @return generated table name, null if the generator fails
     */
    private String generateTableName(String tableName, String attribute){

        //check whether table is in MySQL or not
        boolean isMySqlTable = mySqlConfig.containsTable(tableName.toLowerCase());

        //check whether attribute is in MySQL or not
        boolean isMySqlAttribute;
        if (!isMySqlTable)
            isMySqlAttribute = false;
        else
            isMySqlAttribute = mySqlConfig.containsColumn(tableName.toLowerCase(), attribute.toLowerCase());

        String engine;//storage engine which table is stored in like mysqldb or mongo
        String database;//database name

        if (isMySqlAttribute){
            engine = "mysqldb";
            database = mySqlConfig.getDatabase();
        }else{

            //check whether mongo contains the appropriate collection
            if (!mongoDBConfig.containsCollection(tableName.toLowerCase())){
                String logMsg = String.format("Query accessed non MySQL attribute: %s from table: %s  " +
                                                "But backup MongoDB does not have collection : %s", attribute, tableName, tableName);
                Log.log(logMsg, componentName, Log.ERROR);
                return null;
            }

            engine = "mongo";
            database = mongoDBConfig.getDatabase();
        }

        return String.format("%s.%s.%s", engine, database, tableName.toLowerCase());
    }
}

//SELECT S.SNAME FROM SAILORS S WHERE S.SID =10 AND S.AGE < 20 AND S.SIG = 10
//SELECT S.SNAME FROM SAILORS S WHERE S.SID IN (SELECT S2.SID FROM SAILORS S2 WHERE S2.SID LIKE S.SIG)