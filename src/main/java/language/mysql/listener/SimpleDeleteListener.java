package language.mysql.listener;

import base.SimpleDeleteContainer;
import config.ConfigUtilities;
import language.mysql.translator.SimpleDeleteTranslator;
import language.mysql.utilities.MySqlLexer;
import language.mysql.utilities.MySqlParser;
import language.mysql.utilities.MySqlParserBaseListener;
import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.tree.ParseTreeWalker;

import java.util.HashSet;

public class SimpleDeleteListener extends MySqlParserBaseListener {

    private String alias;
    private String query;

    private HashSet<String> simpleIds;

    private SimpleDeleteContainer simpleDeleteContainer;

    public SimpleDeleteListener(SimpleDeleteContainer simpleDeleteContainer, String query) {
        simpleIds = new HashSet<>();
        this.simpleDeleteContainer = simpleDeleteContainer;
        this.query = query;
    }

    @Override
    public void enterTableName(MySqlParser.TableNameContext ctx) {
        simpleDeleteContainer.setTableName(ctx.getText().toLowerCase());
        simpleDeleteContainer.setAlias(ctx.getText().toLowerCase().charAt(0) + "");
        this.alias = ctx.getText().toLowerCase().charAt(0) + "";
    }

    @Override
    public void enterFullColumnName(MySqlParser.FullColumnNameContext ctx) {
        simpleIds.add(ctx.getText());
    }

    @Override
    public void exitSingleDeleteStatement(MySqlParser.SingleDeleteStatementContext ctx) {
        int whereIndex = query.toLowerCase().indexOf("where");
        String whereClause = query.substring(whereIndex+5).trim();

        StringBuilder finalWhereClause = new StringBuilder();
        String[] words = whereClause.split(" ");
        for(String word : words) {
            if (simpleIds.contains(word))
                finalWhereClause.append(alias).append(".").append(word).append(" ");
            else
                finalWhereClause.append(word).append(" ");
        }

        simpleDeleteContainer.setWhereClause(finalWhereClause.toString().toLowerCase());
    }

    public static void main(String[] args) {
        ConfigUtilities.loadMainConfig("/home/amin/programming/projects/dolus/dolus-config.json");
        ConfigUtilities.loadStorageConfig();

        String query = "DELETE FROM SAILORS WHERE AGE < 10 AND SID = 10";
        CharStream charStream = CharStreams.fromString(query);
        MySqlLexer lexer = new MySqlLexer(charStream);
        CommonTokenStream tokenStream = new CommonTokenStream(lexer);

        MySqlParser parser = new MySqlParser(tokenStream);

        MySqlParser.RootContext rootContext = parser.root();

        SimpleDeleteContainer container = new SimpleDeleteContainer();

        SimpleDeleteListener listener = new SimpleDeleteListener(container,query);

        new ParseTreeWalker().walk(listener, rootContext);

        System.out.println(container);
        SimpleDeleteTranslator translator = new SimpleDeleteTranslator(container);
        System.out.println(translator.generateFirstPhaseSelect());
        int[] ids = {1,2,3,4};
        translator.translate(ids);

    }
}


