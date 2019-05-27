package language.mysql.translator;

import base.SimpleGeneratedTable;
import base.SimpleSelectContainer;
import config.ConfigUtilities;
import config.MongoDBConfigContainer;
import config.MySqlConfigContainer;
import config.StorageConfigContainer;
import language.mysql.utilities.MySqlParser;
import org.antlr.v4.runtime.tree.ParseTree;
import org.antlr.v4.runtime.tree.TerminalNode;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Stack;

public class SimpleSelectTranslator {

    private static final String componentName = "SimpleSelectTranslator";

    private SimpleSelectContainer simpleSelectContainer;

    public SimpleSelectTranslator(SimpleSelectContainer simpleSelectContainer) {
        this.simpleSelectContainer = simpleSelectContainer;
    }

    public String translate(ParseTree root) {

        boolean enteredFrom = false;
        boolean enteredWhere = false;

        StringBuilder translatedSelectQuery = new StringBuilder();

        Stack<ParseTree> stack = new Stack<>();

        stack.add(root);

        //current node to translate
        ParseTree current;

        while (!stack.isEmpty()) {

            current = stack.pop();

            //if reached a reference to a attribute in the form of "alias.attribute"
            if (current instanceof MySqlParser.FullColumnNameContext) {

                String translatedRef = simpleSelectContainer.getTranslatedRef(current.getText());

                translatedSelectQuery.append(translatedRef).append(" ");

                continue;
            }
            //if reached a table reference item in FROM clause like "table_name alias"
            else if (current instanceof MySqlParser.AtomTableItemContext) {

                //get the name of the table
                String tableName = ((MySqlParser.AtomTableItemContext) current).tableName().getText();

                //get the alias of the table
                String alias = ((MySqlParser.AtomTableItemContext) current).alias.getText().toLowerCase();

                HashSet generatedSet = simpleSelectContainer.getGeneratedTableSetFor(tableName);

                Object[] generatedTablesArray = generatedSet.toArray();

                if (enteredFrom)
                    translatedSelectQuery.append(", ");

                for (int i = 0; i < generatedTablesArray.length; i++) {
                    translatedSelectQuery.append(generatedTablesArray[i]).append(" ");

                    translatedSelectQuery.append(generatedTablesArray[i].toString().replaceAll("\\.", "_")).append("_").append(alias);

                    if (i != generatedTablesArray.length - 1)
                        translatedSelectQuery.append(", ");
                    else
                        translatedSelectQuery.append(" ");
                }

                enteredFrom = true;

                continue;
            } else if (current instanceof TerminalNode) {
                if (enteredFrom && current.getText().equals(","))
                    continue;
                else if (current.getText().equals("<EOF>")) {
                    String joinClause = generateJoinClause();
                    if (joinClause != null && joinClause.length() != 0) {
                        translatedSelectQuery.append(" WHERE ");
                        translatedSelectQuery.append(joinClause);
                    }
                }
                else
                    translatedSelectQuery.append(current.getText()).append(" ");

                if (current.getText().toLowerCase().equals("where")) {
                    enteredWhere = true;
                    translatedSelectQuery.append(generateJoinClause());
                }
            }

            //add all children of the visited node to stack
            for (int i = current.getChildCount() - 1; i >= 0; i--)
                stack.push(current.getChild(i));

        }

        return translatedSelectQuery.toString();
    }

    private String getPrimaryKey(StorageConfigContainer container, String collectionName) {

        return container.getPrimaryKey(collectionName);

    }

    private String generateJoinClause() {

        //generate join clause
        StringBuilder joinClauseBuffer = new StringBuilder();

        HashMap<String, HashSet<String>> generatedTables = simpleSelectContainer.getGeneratedTables();

        boolean isGneneratedBefore = false;

        String[] stringType = new String[0];
        String[] keyArray = generatedTables.keySet().toArray(stringType);
        for (int i = 0; i < keyArray.length; i++) {
            if (generatedTables.get(keyArray[i]).size() > 1) {
                if (isGneneratedBefore)
                    joinClauseBuffer.append(" AND ");

                String aliasName = simpleSelectContainer.getAliasNameForTable(keyArray[i]).toLowerCase();

                String primaryKey = getPrimaryKey(ConfigUtilities.findStorage(keyArray[i].toLowerCase()), keyArray[i].toLowerCase());
                String[] generated = generatedTables.get(keyArray[i]).toArray(stringType);

                String firstGeneratedTable = generated[0].replaceAll("\\.", "_") + "_" + aliasName + "." + primaryKey;
                String secondGeneratedTable = generated[1].replaceAll("\\.", "_") + "_" + aliasName + "." + primaryKey;

                joinClauseBuffer.append(firstGeneratedTable).append(" = ").append(secondGeneratedTable);
                isGneneratedBefore = true;
            }
        }
        return joinClauseBuffer.toString();
    }


}
