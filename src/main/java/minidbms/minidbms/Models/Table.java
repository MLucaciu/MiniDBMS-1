package minidbms.minidbms.Models;

import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;
import java.util.List;

@Getter @Setter
public class Table implements Serializable {
    private String tableName;
    private String fileName;
    private String rowLength;
    private List<Attribute> structure;
    private List<String> primaryKeys;
    private List<ForeignKey> foreignKeys;
    private List<IndexFile> indexFiles;

    public Table(String tableName, String fileName, String rowLength, List<Attribute> structure,
                 List<String> primaryKeys, List<IndexFile> indexFiles, List<ForeignKey> foreignKeys){
        this.tableName = tableName;
        this.fileName = fileName;
        this.rowLength = rowLength;
        this.structure = structure;
        this.primaryKeys = primaryKeys;
        this.indexFiles = indexFiles;
        this.foreignKeys = foreignKeys;
    }

    public Table(){}

    public String getTableName() {
        return tableName;
    }

    public List<String> getPrimaryKeys() {
        return primaryKeys;
    }

    public void addindexFile(IndexFile indexFile){
        this.indexFiles.add(indexFile);
    }

    public List<Attribute> getStructure() {
        return structure;
    }

    public List<IndexFile> getIndexFiles() {
        return indexFiles;
    }

    public List<ForeignKey> getForeignKeys() {
        return foreignKeys;
    }

    public Integer indexOfForeignKey(String foreignKeyName){
        for (int i = 0;i<this.foreignKeys.size();i++) {
            if(this.foreignKeys.get(i).getNameForeignKey().equalsIgnoreCase(foreignKeyName)){
                return  i;
            }
        }
        return -1;
    }

    public Integer indexOfAttribute(String attributeName){
        for (int i = 0;i<this.structure.size();i++) {
            if(this.structure.get(0).getAttributeName().equalsIgnoreCase(attributeName)){
                return  i;
            }
        }
        return -1;
    }
}
