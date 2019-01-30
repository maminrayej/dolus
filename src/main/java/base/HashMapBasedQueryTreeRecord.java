package base;

import java.util.ArrayList;
import java.util.HashMap;

/**
 * Implementation of QueryTreeRecord using hash map to store necessary information
 *
 * @author m.amin rayej
 * @version 1.0
 * @since 1.0
 */
public class HashMapBasedQueryTreeRecord extends QueryTreeRecord<String,String> {

    /**
     * Keeps the record of aliases that translator generates for aliases in the original query
     * in order to translate current alias to generated alias for that attribute
     * ( current_alias.attribute_name -> generated_alias.attribute_name )
     */
    private HashMap<String,String> generatedAliasAttributeMap;

    /**
     * Keeps the record of generated tables
     * Dolus needs to generate some tables according to attributes used in the query
     */
    private HashMap<String, ArrayList<String>> generatedTableNamesMap;

    /**
     * Default constructor
     */
    public HashMapBasedQueryTreeRecord(){

        this(null);
    }

    /**
     * @param parent parent of this record
     * @since 1.0
     */
    public HashMapBasedQueryTreeRecord(HashMapBasedQueryTreeRecord parent){

        super(parent, new HashMap<>());

        this.generatedAliasAttributeMap = new HashMap<>();

        this.generatedTableNamesMap = new HashMap<>();
    }

    /**
     * Maps an alias.attribute to a generated_alias.attribute
     *
     * @param aliasAttribute alias.attribute that used in the original query
     * @param generatedAliasAttribute generated_alias.attribute that generated by Dolus
     * @since 1.0
     */
    public void addGeneratedAliasAttribute(String aliasAttribute, String generatedAliasAttribute){

        generatedAliasAttributeMap.put(aliasAttribute, generatedAliasAttribute);

    }

    /**
     * Maps a table name used in the original query to list of generated tables related to the original table
     *
     * @param tableName name of the table in the original query
     * @param generatedTableName generated table
     * @since 1.0
     */
    public void addGeneratedTableName(String tableName, String generatedTableName){

        //if HashMap does not contain the table name already, create a new array list for that table and map them
        if (!generatedTableNamesMap.containsKey(tableName)){

            ArrayList<String> tables = new ArrayList<>();

            tables.add(generatedTableName);

            generatedTableNamesMap.put(tableName, tables);
        }
        else if (!generatedTableNamesMap.get(tableName).contains(generatedTableName)){
            generatedTableNamesMap.get(tableName).add(generatedTableName);
        }
    }

    /**
     * Searches for the generated substitute of the alias.attribute term used in the original query
     *
     * @param aliasAttribute alias.attribute used in the original query
     * @return generated_alias.attribute for the aliasAttribute param or null if not found
     * @since 1.0
     */
    public String getGeneratedAliasAttribute(String aliasAttribute){

        return generatedAliasAttributeMap.getOrDefault(aliasAttribute, null);
    }

    /**
     * Get generated tables for the table name used in the original query
     *
     * @param tableName table name used in the original query
     * @return list of table names generated in substitute of the original table
     * @since 1.0
     */
    public ArrayList<String> getGeneratedTableNames(String tableName){

        return generatedTableNamesMap.getOrDefault(tableName,null);
    }

    /**
     * Get all generated tables
     *
     * @return all generated tables
     * @since 1.0
     */
    public HashMap<String, ArrayList<String>> getGeneratedTableNamesMap() {
        return generatedTableNamesMap;
    }

    @Override
    public String toString(){
        String temp = String.format("Generated Table Aliases : %s\nGenerated Table Names: %s", generatedAliasAttributeMap, generatedTableNamesMap);

        return "{" + super.toString() + "\n" +temp + "}";

    }

}