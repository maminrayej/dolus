package language.mysql.listener;

import base.SimpleInsertContainer;
import config.ConfigUtilities;
import language.mysql.translator.SimpleInsertTranslator;
import language.mysql.filter.SimpleInsertFilter;
import language.mysql.utilities.MySqlLexer;
import language.mysql.utilities.MySqlParser;
import language.mysql.utilities.MySqlParserBaseListener;
import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.tree.ParseTreeWalker;

import java.util.HashMap;
import java.util.LinkedList;

public class SimpleInsertListener extends MySqlParserBaseListener {

    private LinkedList<String> keys;
    private LinkedList<String> values;

    private String tableName;

    private SimpleInsertContainer simpleInsertContainer;

    private boolean enteredUidList = false;

    public SimpleInsertListener(SimpleInsertContainer simpleInsertContainer) {
        keys = new LinkedList<>();
        values = new LinkedList<>();
        this.simpleInsertContainer = simpleInsertContainer;
    }

    @Override
    public void enterTableName(MySqlParser.TableNameContext ctx) {
        this.tableName = ctx.getText().toLowerCase();
    }

    @Override
    public void enterUidList(MySqlParser.UidListContext ctx) {
        enteredUidList = true;
    }

    @Override
    public void enterSimpleId(MySqlParser.SimpleIdContext ctx) {
        if (!enteredUidList)
            return;

        keys.addFirst(ctx.getText().toLowerCase());
    }

    @Override
    public void enterConstant(MySqlParser.ConstantContext ctx) {
        values.addFirst(ctx.getText());
    }

    @Override
    public void exitRoot(MySqlParser.RootContext ctx) {

        HashMap<String,String> keyValuePairs = new HashMap<>();

        for (int i = 0; i < keys.size(); i++) {
            keyValuePairs.put(keys.get(i),values.get(i));
        }

        simpleInsertContainer.setTableName(this.tableName);
        simpleInsertContainer.setKeyValuePairs(keyValuePairs);
    }

    public static void main(String[] args) {

        ConfigUtilities.loadMainConfig("/home/amin/programming/projects/dolus/dolus-config.json");
        ConfigUtilities.loadStorageConfig();

        CharStream charStream = CharStreams.fromString("INSERT INTO SAILORS(SID,SNAME,AGE,NOSQL,NOSQL2) VALUES(1,\"Amin\",21, \"NoSQL\", \"NOSQL2\")");
        MySqlLexer lexer = new MySqlLexer(charStream);
        CommonTokenStream tokenStream = new CommonTokenStream(lexer);

        MySqlParser parser = new MySqlParser(tokenStream);

        MySqlParser.RootContext rootContext = parser.root();

        SimpleInsertContainer container = new SimpleInsertContainer();

        SimpleInsertListener listener = new SimpleInsertListener(container);

        new ParseTreeWalker().walk(listener, rootContext);

        SimpleInsertFilter filter = new SimpleInsertFilter(container);
        filter.filterKeyValues();

        SimpleInsertTranslator translator = new SimpleInsertTranslator(container.getTableName(), filter.getMySqlKeyValuePairs(), filter.getMongoKeyValuePairs());
        translator.translate();

        System.out.println(container);
        System.out.println(filter);
        System.out.println(translator);

    }
}
