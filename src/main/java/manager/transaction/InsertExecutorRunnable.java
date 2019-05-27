package manager.transaction;

import base.SimpleInsertContainer;
import com.mongodb.CommandResult;
import com.mongodb.DB;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoDatabase;
import config.ConfigUtilities;
import config.StorageConfigContainer;
import language.mysql.filter.SimpleInsertFilter;
import language.mysql.listener.SimpleInsertListener;
import language.mysql.translator.SimpleInsertTranslator;
import language.mysql.utilities.MySqlLexer;
import language.mysql.utilities.MySqlParser;
import manager.lock.Lock;
import manager.lock.LockConstants;
import manager.lock.LockManager;
import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.tree.ParseTreeWalker;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.json.simple.JSONObject;


import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;

public class InsertExecutorRunnable extends QueryExecutor{

    private String table;

    private String mySqlInsertQuery;

    private String mongoDBInsertQuery;

    private String query;

    public InsertExecutorRunnable(String query, Transaction transaction) {

        super(transaction);

        this.query = query;
    }
    @Override
    public void run() {
        translateQuery(query);

        String database = getDatabase(this.table);

        //submit lock request for table
        super.submitLock(new Lock(database,table.toLowerCase(), LockConstants.LockTypes.EXCLUSIVE));

        System.out.println("Lock requests are submitted");
        waitOrDie();
        System.out.println("reached after wait or die");

        //execute mysql query
        if (mySqlInsertQuery != null) {
            try
            {
                // create a mysql database connection
                String myDriver = "com.mysql.cj.jdbc.Driver";
                String myUrl = "jdbc:mysql://localhost/sailingmanagement";
                Class.forName(myDriver);
                Connection conn = DriverManager.getConnection(myUrl, "test", "test");
                Statement statement = conn.createStatement();

                System.out.println("Connected to database");

                //execute insert
                System.out.println("Executing insert: " + this.mySqlInsertQuery);
                statement.executeUpdate(this.mySqlInsertQuery);
                System.out.println("Done executing insert");

                System.out.println("Closing the connection");
                conn.close();
            }
            catch (Exception e)
            {
                e.printStackTrace();
            }
        }

        //execute mongodb query
        if (mongoDBInsertQuery != null) {
            System.out.println("Connection to mongodb...");
            MongoClient mongoClient = new MongoClient( "localhost" , 27017 );
            System.out.println("Connected to mongodb");

            System.out.println("Executing insert: " + mongoDBInsertQuery);
            MongoDatabase db = mongoClient.getDatabase( database );
            Bson command = new Document("eval", mongoDBInsertQuery);
            Document result= db.runCommand(command);
            System.out.println("Executed insert: " + mongoDBInsertQuery);
            System.out.println(result.get("ok"));
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

    private void translateQuery(String query) {
        CharStream charStream = CharStreams.fromString(query);

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

        this.mySqlInsertQuery = translator.getMySqlInsertStatement();
        this.mongoDBInsertQuery = translator.getMongoDBInsertStatement();
        this.table = container.getTableName();

    }
}
