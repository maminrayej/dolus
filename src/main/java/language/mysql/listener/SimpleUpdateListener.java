package language.mysql.listener;

import base.SimpleUpdateContainer;
import config.ConfigUtilities;
import language.mysql.filter.SimpleUpdateFilter;
import language.mysql.translator.SimpleUpdateTranslator;
import language.mysql.utilities.MySqlLexer;
import language.mysql.utilities.MySqlParser;
import language.mysql.utilities.MySqlParserBaseListener;
import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.tree.ParseTreeWalker;

import java.util.LinkedList;

public class SimpleUpdateListener extends MySqlParserBaseListener {

    private SimpleUpdateContainer simpleUpdateContainer;

    private boolean exitTableName = false;

    private String originalQuery;

    public SimpleUpdateListener(SimpleUpdateContainer simpleUpdateContainer, String originalQuery) {
        this.simpleUpdateContainer = simpleUpdateContainer;
        this.originalQuery = originalQuery;
    }

    @Override
    public void exitTableName(MySqlParser.TableNameContext ctx) {

        simpleUpdateContainer.setTableName(ctx.getText().toLowerCase());

        exitTableName = true;
    }

    @Override
    public void enterUid(MySqlParser.UidContext ctx) {
        if (exitTableName) {
            exitTableName = false;
            simpleUpdateContainer.setAlias(ctx.getText().toLowerCase());
        }
    }

    @Override
    public void enterUpdatedElement(MySqlParser.UpdatedElementContext ctx) {
        simpleUpdateContainer.addKeyValuePair(ctx.fullColumnName().getText().toLowerCase(), ctx.expression().getText());
    }

    @Override
    public void exitSingleUpdateStatement(MySqlParser.SingleUpdateStatementContext ctx) {
        if (originalQuery.toLowerCase().contains("where")) {
            int index = originalQuery.toLowerCase().indexOf("where");
            simpleUpdateContainer.setWhereClause(originalQuery.substring(index+5).toLowerCase().trim());
        } else {
            simpleUpdateContainer.setWhereClause(null);
        }
    }

    public static void main(String[] args) {

        ConfigUtilities.loadMainConfig("/home/amin/programming/projects/dolus/dolus-config.json");
        ConfigUtilities.loadStorageConfig();

        String query ="UPDATE SAILORS S SET S.SNAME = \"AMIN\", S.AGE = 21 ,S.NOSQL = \"NOSQL\" WHERE S.AGE < 20 AND S.SID < 10";
        CharStream charStream = CharStreams.fromString(query);
        MySqlLexer lexer = new MySqlLexer(charStream);
        CommonTokenStream tokenStream = new CommonTokenStream(lexer);

        MySqlParser parser = new MySqlParser(tokenStream);

        MySqlParser.RootContext rootContext = parser.root();

        SimpleUpdateContainer container = new SimpleUpdateContainer();

        SimpleUpdateListener simpleUpdateListener = new SimpleUpdateListener(container, query);

        new ParseTreeWalker().walk(simpleUpdateListener, rootContext);

        System.out.println(container);
        SimpleUpdateFilter filter = new SimpleUpdateFilter(container);

        filter.filterKeyValuePairs();
        System.out.println(filter);

        SimpleUpdateTranslator translator = new SimpleUpdateTranslator(container,filter);
        System.out.println(translator.getFirstPhaseSelect());
        LinkedList<Integer> ids = new LinkedList<>();
        ids.addFirst(1);
        ids.addFirst(2);
        ids.addFirst(3);
        ids.addFirst(4);
        translator.translateUpdateStatements(ids);
        System.out.println(translator.getMySqlUpdateStatement());
        System.out.println(translator.getMongoUpdateStatement());

    }
}
