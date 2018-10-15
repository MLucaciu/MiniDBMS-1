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
    //foreign keys + references
    //unique keys
    private List<IndexFile> indexFiles;

    public Table(String tableName, String fileName, String rowLength, List<Attribute> structure, List<String> primaryKeys, List<IndexFile> indexFiles){
        this.tableName = tableName;
        this.fileName = fileName;
        this.rowLength = rowLength;
        this.structure = structure;
        this.primaryKeys = primaryKeys;
        this.indexFiles = indexFiles;
    }

    public Table(){}

    public String getTableName() {
        return tableName;
    }

    public void addindexFile(IndexFile indexFile){
        this.indexFiles.add(indexFile);
    }
}
