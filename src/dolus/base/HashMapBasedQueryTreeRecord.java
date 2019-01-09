package dolus.base;

import java.util.ArrayList;
import java.util.HashMap;

/**
 * Implementation of QueryTreeRecord using hash map to store necessary information
 *
 * @param <K> Type of the keys in symbol table
 * @param <V> Type of the values in symbol table
 * @author m.amin rayej
 * @version 1.0
 * @since 1.0
 */
public class HashMapBasedQueryTreeRecord<K,V> extends QueryTreeRecord<K,V> {

    /**
     * Keeps the record of aliases that translator generates for attributes
     * in order to translate current alias to generated alias for that attribute
     * ( current_alias.attribute_name -> generated_alias.attribute_name )
     */
    private HashMap<String,String> generatedAliasAttributeMap;

    /**
     * Keeps the record of generated tables
     * Dolus needs to generate some tables according to attributes used in the query
     */
    private HashMap<String, ArrayList<String>> generatedTablesMap;

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
    public HashMapBasedQueryTreeRecord(HashMapBasedQueryTreeRecord<K,V> parent){

        super(parent, new HashMap<>());

        this.generatedAliasAttributeMap = new HashMap<>();

        this.generatedTablesMap = new HashMap<>();
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
     * @param generatedTable generated table
     * @since 1.0
     */
    public void addGeneratedTable(String tableName, String generatedTable){

        //if HashMap does not contain the table name already, create a new array list for that table and map them
        if (!generatedTablesMap.containsKey(tableName)){

            ArrayList<String> tables = new ArrayList<>();

            tables.add(generatedTable);

            generatedTablesMap.put(tableName, tables);
        }
        else{
            generatedTablesMap.get(tableName).add(generatedTable);
        }
    }

    /**
     * Searches for the generated substitute of the alias.attribute term used in the original query
     *
     * @param aliasAttribute alias.attribute used in the original query
     * @return generated_alias.attribute for the aliasAttribute param
     * @since 1.0
     */
    public String getGeneratedAlias(String aliasAttribute){

        return generatedAliasAttributeMap.getOrDefault(aliasAttribute, null);
    }

    /**
     * Get generated tables for the table name used in the original query
     *
     * @param tableName table name used in the original query
     * @return list of table names generated in substitute of the original table
     * @since 1.0
     */
    public ArrayList<String> getGeneratedTable(String tableName){

        return generatedTablesMap.getOrDefault(tableName,null);
    }

}
