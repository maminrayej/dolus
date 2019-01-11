package dolus.language.mysql.visitor;

import dolus.base.HashMapBasedQueryTreeRecordManager;
import dolus.config.MongoDBConfig;
import dolus.config.MySqlConfig;
import dolus.language.mysql.utilities.MySqlParser.AtomTableItemContext;
import dolus.language.mysql.utilities.MySqlParser.FullColumnNameContext;
import dolus.language.mysql.utilities.MySqlParser.QuerySpecificationContext;
import org.antlr.v4.runtime.RuleContext;
import org.antlr.v4.runtime.tree.ParseTree;
import org.antlr.v4.runtime.tree.TerminalNode;

import java.util.ArrayList;
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
    private MySqlConfig mySqlConfig;

    /**
     * Meta data and configuration of MongoDB
     */
    private MongoDBConfig mongoDBConfig;

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
                             MySqlConfig mySqlConfig, MongoDBConfig mongoDBConfig) {

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
     */
    public String translate() {

        //result of the translation
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
     */
    private void translateColumnNameReference(FullColumnNameContext current, StringBuilder result) {

        //ask the record manager to find the generated_alias.attribute for the original alias.attribute in its records
        String generatedAlias = recordManager.getGeneratedTableAlias(current.getText());

        //append an whitespace for better printing
        result.append(generatedAlias).append(" ");
    }

    /**
     * Translates a table name to its generated ones
     *
     * @param current current node in the parse tree
     * @param result  translated content
     */
    private void translateTableName(AtomTableItemContext current, StringBuilder result) {

        //get the name of the table
        String tableName = current.tableName().getText();

        //get the alias of the table
        String alias = current.alias.getText();

        //ask the record manager to search for the generated table corresponds to the table name
        ArrayList<String> generatedTableNames = recordManager.getGeneratedTableNames(tableName);

        //append the generated tables to the already translated content
        for (int i = 0; i < generatedTableNames.size(); i++) {

            String generatedTableName = generatedTableNames.get(i);

            //generate an alias for the generated table name by replacing all "." characters with "_"
            String generatedTableAlas = generatedTableName.replaceAll("\\.", "_");

            //if it's the first element -> don't print comma before the the generated table name
            //at the end it looks like this : generated_table_name generated_table_alias , ...
            if (i == 0)
                result.append(generatedTableName).append(" ").append(generatedTableAlas).append("_").append(alias.toLowerCase()).append(" ");
            else
                result.append(",").append(generatedTableName).append(" ").append(generatedTableAlas).append("_").append(alias.toLowerCase()).append(" ");
        }
    }

}
