package base;

import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.function.Consumer;

public class SimpleSelectContainer {

    private HashMap<String,String> symbolTable;

    private HashMap<String,String> reverseSymbolTable;

    private HashMap<String,String> translatedRefs;

    private HashMap<String, HashSet<String>> generatedTables;

    public SimpleSelectContainer() {
        this.symbolTable = new HashMap<>();
        this.reverseSymbolTable = new HashMap<>();
        this.translatedRefs = new HashMap<>();
        this.generatedTables = new HashMap<>();
    }

    public void addAliasTableMap(String tableName, String alias) {
        symbolTable.put(alias, tableName);
        reverseSymbolTable.put(tableName, alias);
    }

    public String getTableNameWithAlias(String alias) {
        return symbolTable.get(alias);
    }

    public String getAliasNameForTable(String tableName) { return reverseSymbolTable.get(tableName); }

    public void addTranslatedRef(String original, String translated) {
        translatedRefs.put(original, translated);
    }

    public String getTranslatedRef(String original) {
        return translatedRefs.get(original);
    }

    public void addGeneratedTableFor(String tableName, String generatedTableName) {

        if (!generatedTables.containsKey(tableName)) {
            HashSet<String> generatedTableList = new HashSet<>();
            generatedTableList.add(generatedTableName);
            generatedTables.put(tableName, generatedTableList);
        }
        else {
            generatedTables.get(tableName).add(generatedTableName);
        }
    }

    public HashSet<String> getGeneratedTableSetFor(String tableName) {
        return generatedTables.get(tableName);
    }

    public HashMap<String, HashSet<String>> getGeneratedTables() {
        return generatedTables;
    }

    public String printSymbolTable() {
        StringBuilder buffer = new StringBuilder();

        symbolTable.keySet().forEach( (string) -> {
            buffer.append(String.format("%s : %s\n", string, symbolTable.get(string)));
        });

        return buffer.toString();
    }

    public String printTranslatedRefs() {
        StringBuilder buffer = new StringBuilder();

        translatedRefs.keySet().forEach( (string) -> {
            buffer.append(String.format("%s : %s\n", string, translatedRefs.get(string)));
        });

        return buffer.toString();
    }

    public String printGeneratedTables() {
        StringBuilder buffer = new StringBuilder();

        generatedTables.keySet().forEach( (string) -> {
            buffer.append(string + ": ");
            generatedTables.get(string).forEach((item) -> {
                buffer.append(item + ",");
            });
            buffer.append("\n");
        });

        return buffer.toString();
    }
}
