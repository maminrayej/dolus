package language.mysql.visitor;

import base.HashMapBasedQueryTreeRecordManager;
import config.MongoDBConfigContainer;
import config.MySqlConfigContainer;
import language.mysql.utilities.MySqlParser.AtomTableItemContext;
import language.mysql.utilities.MySqlParser.FromClauseContext;
import language.mysql.utilities.MySqlParser.FullColumnNameContext;
import language.mysql.utilities.MySqlParser.QuerySpecificationContext;
import org.antlr.v4.runtime.RuleContext;
import org.antlr.v4.runtime.tree.ParseTree;
import org.antlr.v4.runtime.tree.TerminalNode;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Stack;

/**
 * This class traverses through the parse tree and translates necessary nodes
 *
 * @author m.amin rayej
 * @version 1.0
 * @since 1.0
 */
public class TranslatorVisitor {

    /**
     * Root of the parse tree
     */
    private ParseTree root;

    /**
     * Record manager
     */
    private HashMapBasedQueryTreeRecordManager recordManager;

    /**
     * Meta data and configurations of MySQL
     */
    private MySqlConfigContainer mySqlConfig;

    /**
     * Meta data and configuration of MongoDB
     */
    private MongoDBConfigContainer mongoDBConfig;

    /**
     * Indicates whether the first select clause is reached or not
     */
    private boolean visitedFirstSelectClause;

    /**
     * Indicates whether this is the first time select being visited or not
     */
    private boolean isFirstSelect;

    /**
     * Keep the depth of the select clause root nodes
     * Each new select node causes a push
     * And each time we exit a select clause causes a pop
     */
    private Stack<Integer> selectQueryDepthStack;

    public TranslatorVisitor(ParseTree root, HashMapBasedQueryTreeRecordManager recordManager,
                             MySqlConfigContainer mySqlConfig, MongoDBConfigContainer mongoDBConfig) {

        this.root = root;
        this.recordManager = recordManager;
        this.mySqlConfig = mySqlConfig;
        this.mongoDBConfig = mongoDBConfig;

        this.visitedFirstSelectClause = false;
        this.isFirstSelect = true;
        this.selectQueryDepthStack = new Stack<>();
    }

    /**
     * Translates the parse tree
     *
     * @since 1.0
     */
    public String translate() {

        StringBuilder result = new StringBuilder();

        return recursiveTranslate(root, result);
        /*//result of the translation
        StringBuilder result = new StringBuilder();

        //stack to traverse the parse tree
        Stack<ParseTree> stack = new Stack<>();

        stack.add(root);

        //current node to translate
        ParseTree current;

        while (!stack.isEmpty()) {

            //ge the next node to visit
            current = stack.pop();

            //manage the depth of the record manager
            manageDepth(current);

            //if reached a reference to a attribute in the form of "alias.attribute"
            if (current instanceof FullColumnNameContext) {

                translateColumnNameReference((FullColumnNameContext) current, result);
                continue;
            }
            //if reached a table reference item in FROM clause like "table_name alias"
            else if (current instanceof AtomTableItemContext) {

                translateTableName((AtomTableItemContext) current, result);
                continue;
            }
            else if (current instanceof FromClauseContext){

                String where = ((FromClauseContext) current).WHERE().getText();

                if (where == null)
                    System.out.println("No Where clause in from clause at depth: "+ ((FromClauseContext) current).depth());
                else
                    System.out.println("Where clause in from clause at depth: "+ ((FromClauseContext) current).depth());

            }
            //if reached a terminal node, print it
            else if (current instanceof TerminalNode) {
                result.append(current.getText()).append(" ");
            }

            //add all children of the visited node to stack
            for (int i = current.getChildCount() - 1; i >= 0; i--)
                stack.push(current.getChild(i));
        }

        return result.toString();*/
    }

    private String recursiveTranslate(ParseTree root, StringBuilder result) {

        //stack to traverse the parse tree
        Stack<ParseTree> stack = new Stack<>();

        stack.add(root);

        //current node to translate
        ParseTree current;

        while (!stack.isEmpty()) {

            //ge the next node to visit
            current = stack.pop();

            //manage the depth of the record manager
            manageDepth(current);

            //if reached a reference to a attribute in the form of "alias.attribute"
            if (current instanceof FullColumnNameContext) {

                translateColumnNameReference((FullColumnNameContext) current, result);
                continue;
            }
            //if reached a table reference item in FROM clause like "table_name alias"
            else if (current instanceof AtomTableItemContext) {

                translateTableName((AtomTableItemContext) current, result);
                continue;
            }
            //if reached a from clause
            else if (current instanceof FromClauseContext) {

                /*
                 * lower bound indicates which children of from clause should be traversed.
                 * from clause can have multiple children like "FROM" "table sources" "WHERE" "expression1" "," "expression2" "GROUP BY" ,...
                 * every child is indexed 0 - ... from left to right
                 * if from clause does not have any where clause then children are like 0:"FROM" 1:"table sources" 2:"GROUP BY" ...
                 * so the lower bound must be 2 so that all children from 2 till end get traversed
                 * but if clause has WHERE then its different like this: 0:"FROM" 1:"table sources" 2:"WHERE" 3:"GROUP BY" ...
                 * so the lower bound of children must be 3 so that after "WHERE" all children get traversed
                 */
                int lowerBound = 2;

                //generated where clause that joins the generated tables
                String generatedJoinClause;

                //start appending from clause by adding "FROM "
                result.append("FROM").append(" ");

                //translate all table declarations and append it to result
                //so after that we have "FROM table1 table_alias1, table2 table_alias2,..."
                recursiveTranslate(((FromClauseContext) current).tableSources(), result);

                //start translating the where clause

                //if there is no where clause
                if (((FromClauseContext) current).WHERE() == null) {

                    //generate the where clause to join generated tables
                    generatedJoinClause = generateJoinClause();

                    //if there is a generated append " WHERE <generated_join> " to the already translated content
                    //if there is not then this from clause does not have any where clause(original or generated)
                    if (generatedJoinClause.length() != 0)
                        result.append(" WHERE ").append(generatedJoinClause).append(" ");

                } else {

                    lowerBound = 3;

                    generatedJoinClause = generateJoinClause();

                    //we are sure that the from clause has at least one where expression so append the " WHERE " keyword
                    result.append(" WHERE ");

                    //if there is any generated joins, append it as the first part of the where conditions
                    //and append an " AND " to combine the generated where clause with original where clause
                    if (generatedJoinClause.length() != 0)
                        result.append(generatedJoinClause).append(" AND ");

                }

                //append all other children of from clause to the stack, so they get traversed as well
                for (int i = current.getChildCount() - 1; i >= lowerBound; i--)
                    stack.push(current.getChild(i));

                continue;

            }
            //if reached a terminal node, print it
            else if (current instanceof TerminalNode) {
                result.append(current.getText()).append(" ");
            }

            //add all children of the visited node to stack
            for (int i = current.getChildCount() - 1; i >= 0; i--)
                stack.push(current.getChild(i));
        }

        return result.toString();
    }

    /**
     * Manually manages the depth of the record manager.
     *
     * @param current current node in the parse tree
     * @since 1.0
     */
    private void manageDepth(ParseTree current) {

        //depth of the current node
        int currentDepth;

        //if reached a new select query
        if (current instanceof QuerySpecificationContext) {

            //check whether if its the first select clause int the query -> if true, don't increment the depth of the record manager
            if (!isFirstSelect)
                recordManager.incrementDepth();
            else
                isFirstSelect = false;

            //keep depth of the select query
            selectQueryDepthStack.push(((QuerySpecificationContext) current).depth());

            System.out.println("Entered a select in depth of " + selectQueryDepthStack.peek());

            //declare that first select statement has been visited
            visitedFirstSelectClause = true;
        }
        //if reached a rule node that is not a query specification node and the first query specification node is visited
        else if (current instanceof RuleContext && visitedFirstSelectClause) {

            //get the depth of the rule node
            currentDepth = ((RuleContext) current).depth();

            //if depth of the rule node is less or equal than the depth of the latest select, this means we exited the latest select
            //so decrement the depth of the record manager
            if (currentDepth <= selectQueryDepthStack.peek()) {

                //ask the record manager to activate the parent record
                recordManager.decrementDepth();

                //discard the depth of the query we exited
                selectQueryDepthStack.pop();

                System.out.println("Exiting a select in depth " + currentDepth);
            }
        }
    }

    /**
     * Translates original alias.attribute to generated_alias.attribute
     *
     * @param current current node in the parse tree
     * @param result  translated content
     * @since 1.0
     */
    private void translateColumnNameReference(FullColumnNameContext current, StringBuilder result) {

        //extract the alias from alias.attribute
        String alias = current.uid().getText();

        //ask the record manager to find the generated_alias.attribute for the original alias.attribute in its records
        String generatedAlias = recordManager.getGeneratedAliasAttribute(alias, current.getText());

        //append an whitespace for better printing
        result.append(generatedAlias).append(" ");
    }

    /**
     * Translates a table name to its generated ones
     *
     * @param current current node in the parse tree
     * @param result  translated content
     * @since 1.0
     */
    private void translateTableName(AtomTableItemContext current, StringBuilder result) {

        //get the name of the table
        String tableName = current.tableName().getText();

        //get the alias of the table
        String alias = current.alias.getText();

        //ask the record manager to search for the generated table corresponds to the table name
        ArrayList<String> generatedTableNames = recordManager.getGeneratedTableNames(tableName);

        int index;

        //append the generated tables to the already translated content
        for (int i = 0; i < generatedTableNames.size(); i++) {

            String generatedTablesAlias = generatedTableNames.get(i);

            //generate an alias for the generated table name by replacing all "." characters with "_"
            String generatedTableName = generatedTablesAlias.replaceAll("_", ".");

            index = generatedTableName.lastIndexOf('.');

            generatedTableName = generatedTableName.substring(0, index);

            //if it's the first element -> don't print comma before the the generated table name
            //at the end it looks like this : generated_table_name generated_table_alias , ...
            if (i == 0)
                result.append(generatedTableName).append(" ").append(generatedTablesAlias);
            else
                result.append(" ,").append(generatedTableName).append(" ").append(generatedTablesAlias).append(" ");
        }
    }


    /**
     * Generates a join clause and joins the related generated tables
     *
     * @return a sub-where clause that joins the related generated tables on their primary keys
     * @since 1.0
     */
    private String generateJoinClause() {

        //translated content
        StringBuilder result = new StringBuilder();

        //all generated tables for the current active query
        HashMap<String, ArrayList<String>> generatedTableNamesMap = recordManager.getGeneratedTableNamesMap();

        //list of table names generated for a table in the original query
        ArrayList<String> tableNames;

        //name of a table
        String tableName;

        //primary key of a table
        String primaryKey;

        //first table name and primary key
        //this method joins all other tables in a category with the first table of that category
        String firstTableName;
        String firstTablePrimaryKey;

        //indicates if this is the first key(table name) loop is processing
        boolean isFirstKey = true;

        //each key in generatedTableNamesMap is a table name.
        //that maps that table name to a list containing generated tables for that table
        for (String key : generatedTableNamesMap.keySet()) {

            //get generated tables for that key(table name in original query)
            tableNames = generatedTableNamesMap.get(key);

            //if there is only one or less table then the generator just renamed the original table
            //there is no need to join any tables
            if (tableNames.size() <= 1)
                continue;

            //get first table name and its primary key
            firstTableName = tableNames.get(0);

            //if table refers a table in language.mysql
            if (firstTableName.contains("mysqldb"))
                firstTablePrimaryKey = mySqlConfig.getPrimaryKey(key.toLowerCase());
            else
                firstTablePrimaryKey = mongoDBConfig.getPrimaryKey(key.toLowerCase());

            //join each table with the first table
            for (int i = 1; i < tableNames.size(); i++) {

                //get the table name
                tableName = tableNames.get(i);

                //if table refers a table in language.mysql
                if (tableName.contains("mysqldb"))
                    primaryKey = mySqlConfig.getPrimaryKey(key.toLowerCase());
                else
                    primaryKey = mongoDBConfig.getPrimaryKey(key.toLowerCase());

                //if this is the first table being joined with the first table
                //there is no need to AND this expression with previous expressions
                //joins like: table1.pk1 = table2.pk2
                if (i == 1 && isFirstKey) {
                    result.append(firstTableName).append(".").append(firstTablePrimaryKey).append(" = ").
                            append(tableName).append(".").append(primaryKey).append(" ");

                    isFirstKey = false;
                } else
                    result.append(" AND ").
                            append(firstTableName).append(".").append(firstTablePrimaryKey).append(" = ").
                            append(tableName).append(".").append(primaryKey);
            }
        }

        return result.toString();
    }

}