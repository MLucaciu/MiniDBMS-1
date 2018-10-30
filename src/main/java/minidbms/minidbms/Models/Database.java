package minidbms.minidbms.Models;

import lombok.Getter;
import lombok.Setter;

import java.util.ArrayList;
import java.util.List;

@Getter
@Setter
public class Database{
    private String dbName;
    private List<Table> tables = new ArrayList<>();

    public Database(String dbName){
        this.dbName = dbName;
    }

    public boolean addTable(Table newTable){
        return this.tables.add(newTable);
    }

    public String getDbName() {
        return dbName;
    }

    public List<Table> getTables() {
        return tables;
    }

}
