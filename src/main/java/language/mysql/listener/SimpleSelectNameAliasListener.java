package language.mysql.listener;

import base.SimpleSelectContainer;
import language.mysql.utilities.MySqlLexer;
import language.mysql.utilities.MySqlParser;
import language.mysql.utilities.MySqlParserBaseListener;
import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.tree.ParseTreeWalker;

public class SimpleSelectNameAliasListener extends MySqlParserBaseListener {

    private SimpleSelectContainer simpleSelectContainer;

    public SimpleSelectNameAliasListener(SimpleSelectContainer simpleSelectContainer) {
        this.simpleSelectContainer = simpleSelectContainer;
    }

    @Override
    public void enterAtomTableItem(MySqlParser.AtomTableItemContext ctx) {

        //extract the table name and its alias
        String tableName = ctx.tableName().getText();
        String alias = ctx.alias.getText();

        //add the symbol to the active record symbol table
        simpleSelectContainer.addAliasTableMap(tableName, alias);
    }

}
