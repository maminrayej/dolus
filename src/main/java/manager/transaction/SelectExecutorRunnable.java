package manager.transaction;

import base.SimpleSelectContainer;
import config.ConfigUtilities;
import config.StorageConfigContainer;
import language.mysql.listener.SimpleSelectGeneratorListener;
import language.mysql.listener.SimpleSelectNameAliasListener;
import language.mysql.translator.SimpleSelectTranslator;
import language.mysql.utilities.MySqlLexer;
import language.mysql.utilities.MySqlParser;
import manager.lock.Lock;
import manager.lock.LockConstants;
import manager.lock.LockManager;
import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.tree.ParseTreeWalker;
import manager.lock.LockConstants.LockTypes;
import org.apache.drill.jdbc.Driver;

import java.sql.*;
import java.util.HashMap;

public class SelectExecutorRunnable extends QueryExecutor{

    private String[] tables;

    private String query;

    public SelectExecutorRunnable(String query, Transaction transaction) {

        super(transaction);

        this.query = query;
    }

    @Override
    public void run() {

        String translatedQuery = getTranslatedSelect(query);

        //for each table request Shared lock
        String database = getDatabase(tables[0]);

        //submit lock request for each table
        for (int i = 0; i < tables.length; i++) {
            super.submitLock(new Lock(database,tables[i].toLowerCase(), LockTypes.EXCLUSIVE));
        }

        System.out.println("Lock requests are submitted");
        waitOrDie();
        System.out.println("reached after wait or die");

        try {
            Class.forName("org.apache.drill.jdbc.Driver");
            final Connection conn = DriverManager.getConnection("jdbc:drill:drillbit=localhost");
            System.out.println("class loaded");

            Statement stmt = conn.createStatement();
            /* Perform a select on data in the classpath storage plugin. */
            String sql = translatedQuery;

            System.out.println("Running: " + sql);
            ResultSet rs = stmt.executeQuery(sql);

            while(rs.next()) {
                String sid = rs.getString("sid");
                String first = rs.getString("sname");
                String phone = rs.getString("phone");

                System.out.println("sid: " + sid);
                System.out.println("Sname: " + first);
                System.out.println("Phone: " + phone);
            }

            rs.close();
            stmt.close();
            conn.close();

        }
        catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        catch (SQLException e1) {
            e1.printStackTrace();
        }

        super.releaseLock();

    }

    private String getDatabase(String tableName) {

        StorageConfigContainer configContainer = ConfigUtilities.findStorage(tableName.toLowerCase());

        if (configContainer != null)
            return configContainer.getDatabase();
        else
            return null;
    }
    private String getTranslatedSelect(String query) {

        CharStream charStream = CharStreams.fromString(query);

        MySqlLexer lexer = new MySqlLexer(charStream);

        CommonTokenStream tokenStream = new CommonTokenStream(lexer);

        MySqlParser parser = new MySqlParser(tokenStream);

        MySqlParser.RootContext rootContext = parser.root();

        SimpleSelectContainer container = new SimpleSelectContainer();

        SimpleSelectNameAliasListener simpleSelectNameAliasListener = new SimpleSelectNameAliasListener(container);

        SimpleSelectGeneratorListener simpleSelectGeneratorListener = new SimpleSelectGeneratorListener(container);

        new ParseTreeWalker().walk(simpleSelectNameAliasListener, rootContext);

        new ParseTreeWalker().walk(simpleSelectGeneratorListener, rootContext);

        SimpleSelectTranslator translator = new SimpleSelectTranslator(container);

        String[] stringType = new String[0];
        this.tables = container.getGeneratedTables().keySet().toArray(stringType);

        return  translator.translate(rootContext);
    }
}
