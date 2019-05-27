package language.mysql.listener;

import base.SimpleSelectContainer;
import common.Log;
import config.ConfigUtilities;
import config.StorageConfigContainer;
import language.mysql.translator.SimpleSelectTranslator;
import language.mysql.utilities.MySqlLexer;
import language.mysql.utilities.MySqlParser;
import language.mysql.utilities.MySqlParserBaseListener;
import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.tree.ParseTreeWalker;

public class SimpleSelectGeneratorListener extends MySqlParserBaseListener {

    private static final String componentName = "SimpleSelectGeneratorListener";

    private SimpleSelectContainer simpleSelectContainer;

    public SimpleSelectGeneratorListener(SimpleSelectContainer simpleSelectContainer) {
        this.simpleSelectContainer = simpleSelectContainer;
    }

    @Override
    public void enterFullColumnName(MySqlParser.FullColumnNameContext ctx) {

        //extract the alias from alias.attribute
        String alias = ctx.uid().getText();

        //extract dotted attribute ".attribute"
        String dottedAttribute = ctx.dottedId(0).getText();

        //just get the attribute and discard the "."
        String attribute = dottedAttribute.substring(1);

        //find the table name the alias is referring to
        String tableName = simpleSelectContainer.getTableNameWithAlias(alias);

        //if alias does not refer to any table, log an error message
        if (tableName == null){
            String logMsg = String.format("Query accessed alias: %s but alias does not refer to any table in the query", alias);
            Log.log(logMsg, componentName, Log.ERROR);
            return;
        }

        StorageConfigContainer storage = ConfigUtilities.findStorage(tableName.toLowerCase(), attribute.toLowerCase());

        if (storage == null) {
            String logMsg = String.format("No storage contains %s.%s", tableName, attribute);
            Log.log(logMsg, componentName, Log.ERROR);
            return;
        }

        String storageId = storage.getId();
        String database = storage.getDatabase();

        String translatedRef = (storageId + "_" + database + "_" + tableName + "_" + alias + "." + attribute).toLowerCase();
        String originalRef   = alias + "." + attribute;
        simpleSelectContainer.addTranslatedRef(originalRef, translatedRef);

        String translatedTable = (storageId + "." + database + "." + tableName).toLowerCase();
        simpleSelectContainer.addGeneratedTableFor(tableName, translatedTable);

    }

    public static void main(String[] args) {

        ConfigUtilities.loadMainConfig("/home/amin/programming/projects/dolus/dolus-config.json");
        ConfigUtilities.loadStorageConfig();

        CharStream charStream = CharStreams.fromString("SELECT S.SNAME, S.PHONE FROM SAILORS S");
        MySqlLexer lexer = new MySqlLexer(charStream);
        CommonTokenStream tokenStream = new CommonTokenStream(lexer);

        MySqlParser parser = new MySqlParser(tokenStream);

        MySqlParser.RootContext rootContext = parser.root();

        SimpleSelectContainer container = new SimpleSelectContainer();

        SimpleSelectNameAliasListener simpleSelectNameAliasListener = new SimpleSelectNameAliasListener(container);

        SimpleSelectGeneratorListener simpleSelectGeneratorListener = new SimpleSelectGeneratorListener(container);

        new ParseTreeWalker().walk(simpleSelectNameAliasListener, rootContext);

        new ParseTreeWalker().walk(simpleSelectGeneratorListener, rootContext);

        System.out.println(container.printSymbolTable());
        System.out.println(container.printTranslatedRefs());
        System.out.println(container.printGeneratedTables());

        System.out.println(new SimpleSelectTranslator(container).translate(rootContext));
    }
}
