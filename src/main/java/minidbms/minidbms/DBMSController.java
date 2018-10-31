package minidbms.minidbms;

import com.fasterxml.jackson.databind.ObjectMapper;
import minidbms.minidbms.Models.Attribute;
import redis.clients.jedis.Jedis;
import minidbms.minidbms.Models.Database;
import minidbms.minidbms.Models.IndexFile;
import minidbms.minidbms.Models.Table;
import org.json.JSONException;
import org.springframework.web.bind.annotation.*;
import java.io.File;
import java.util.*;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

@RestController
@CrossOrigin(origins = "*")
public class DBMSController {

    private List<Database> databases;
    private ObjectMapper mapper;
    Jedis jedis;

    private String PATH_TO_JSON = "D:\\chestii\\1 - isgbd\\database.json";
    /**
     *
     */
    DBMSController(){
        mapper = new ObjectMapper();
        databases = new ArrayList<>();

        Jedis jedis = new Jedis("localhost");
        jedis.set("foo", "bar");
        String value = jedis.get("foo");
        HashMap hm = new HashMap<String,String>();
        hm.put("100","Amit");
        hm.put("1","cecece");
        String ce = jedis.hmset("ce", hm);


        Map tablesData = jedis.hgetAll("ce");
        Object check = tablesData.get("1");
        tablesData.remove("1");
        jedis.hmset("ce",tablesData);

        Object tablesData2 = jedis.hget("ce","1");
        jedis.hdel("ce","1");
        Object tablesData3 = jedis.hget("ce","1");
        Map tablesData4 = jedis.hgetAll("ce");
        int ceva =3 ;

    }

    @RequestMapping(value = "/createDatabase", method = RequestMethod.POST)
    @ResponseBody
    public String createDataBase(@RequestParam(value="dbName", required = true) String dbName) throws IOException, JSONException {
        databases.add(new Database(dbName));
        mapper.writeValue(new File(PATH_TO_JSON), databases );
        return "Success!";
    }

    /*
    Table Name
    Attribute: attributes -> AttributeName + Type + length + isNull
    Primary Key:
     */
    @RequestMapping(value = "/createTable", method = RequestMethod.POST)
    public String createTable(@RequestParam(value="dbName", required = true) String dbName,
                              @RequestBody(required = false) String table){
        String result;
        Table newTable;
        Jedis jedis = new Jedis("localhost");
        Map tablesData4 = jedis.hgetAll("ce");
        try {
            result = java.net.URLDecoder.decode(table.substring(table.indexOf("&")+1), "UTF-8");
            result = result.substring(0, result.length() - 1);
            newTable = new ObjectMapper().readValue(result, Table.class);
            Database database = this.databases.stream().filter(db -> db.getDbName().equalsIgnoreCase(dbName)).findFirst().orElse(null);
            database.addTable(newTable);
            mapper.writeValue(new File(PATH_TO_JSON), databases );
            HashMap hm = new HashMap <String,String>();
            hm.put("database",database.getDbName());
            jedis.hmset(newTable.getTableName(), hm);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return "Success!";
    }

    @RequestMapping(value = "/dropDatabase", method = RequestMethod.DELETE)
    public String dropDatabase(@RequestParam(value="dbName", required = true) String dbName) throws IOException {
        Database database = this.databases.stream().filter(db -> db.getDbName().equalsIgnoreCase(dbName)).findFirst().orElse(null);

        if(database != null){
            this.databases.remove(database);
            mapper.writeValue(new File(PATH_TO_JSON), databases );
    }

        return "Success";
    }

    @RequestMapping(value = "/dropTable", method = RequestMethod.DELETE)
    public String dropTable(@RequestParam(value="tableName", required = true) String tableName,
                            @RequestParam(value="dbName", required = true) String dbName) throws IOException {
        Database database = this.databases.stream().filter(db -> db.getDbName().equalsIgnoreCase(dbName)).findFirst().orElse(null);

        if(database != null){
            Table table = database.getTables().stream().filter(tb -> tb.getTableName().equalsIgnoreCase(tableName)).findFirst().orElse(null);
            if(table != null){
                database.getTables().remove(table);
                mapper.writeValue(new File(PATH_TO_JSON), databases );
            }
        }

        return "Success";
    }

    /*
    IndexFile: indexName + keyLength + isUnique + indexType
        IndexAttributes -> Attributes
     */
    @RequestMapping(value = "/createIndex", method = RequestMethod.POST)
    public String createIndex(@RequestParam(value="dbName", required = true) String dbName,
                              @RequestParam(value="tableName", required = true) String tableName,
                              @RequestBody(required = false) String indexFile){
        String result;
        IndexFile newIndexFile;
        try {
            result = java.net.URLDecoder.decode(indexFile.substring(indexFile.lastIndexOf("&")+1), "UTF-8");
            result = result.substring(0, result.length() - 1);
            newIndexFile = new ObjectMapper().readValue(result, IndexFile.class);
            Database database = this.databases.stream().filter(db -> db.getDbName().equalsIgnoreCase(dbName)).findFirst().orElse(null);
            Table table = database.getTables().stream().filter(tb -> tb.getTableName().equalsIgnoreCase(tableName)).findFirst().orElse(null);
            table.addindexFile(newIndexFile);
            mapper.writeValue(new File(PATH_TO_JSON), databases );
        } catch (IOException e) {
            e.printStackTrace();
        }
        return "Success!";
    }

    @RequestMapping(value = "/insert", method = RequestMethod.POST)
    public String insert(@RequestParam(value="dbName", required = true) String dbName,
                         @RequestParam(value="tableName", required = true) String tableName,
                         @RequestBody String values) throws UnsupportedEncodingException {

        Map map = new HashMap();
        //check type, length and IsNull
        //first primary keys; second foreign keys, attributes
        String result = java.net.URLDecoder.decode(values.substring(values.lastIndexOf("&")+1), "UTF-8");
        result = result.substring(1, result.length() - 2);
        String[] entities = result.split("#");
        Jedis jedis = new Jedis("localhost");
        Map tablesData4 = jedis.hgetAll("ce");

        Database database = this.databases.stream().filter(db -> db.getDbName().equalsIgnoreCase(dbName)).findFirst().orElse(null);
        Table table = database.getTables().stream().filter(tb -> tb.getTableName().equalsIgnoreCase(tableName)).findFirst().orElse(null);

        //check if the primary key is unique
        int noPrimaryKeys = table.getPrimaryKeys().size();
        for(int i = 0; i < noPrimaryKeys ; i++){
            String primaryKey = entities[i];
            if(table.getPrimaryKeys().stream().filter(pk -> pk.equals(primaryKey)).count() != 0){
                return "Primary key is not unique";
            }
        }

        //check for foreign keys

        //check attributes(type,length,IsNull)
        int indexAtt = -1;
        for (Attribute attribute:table.getStructure()){
            indexAtt++;
            //type: string, integer
            if(attribute.getType().equals("Integer")){
                try{
                    Integer.parseInt(entities[indexAtt]);
                } catch (NumberFormatException e) {
                    return "Attribute not an integer!";
                }
            } else if(attribute.getType().equals("String")){
                //length
                int lengthAtt = Integer.parseInt(attribute.getLength());
                if (entities[indexAtt].length() > lengthAtt){
                    return "Attribute too long";
                }
            }

            //IsNull
            if(attribute.getIsNull().equals("False") && entities[indexAtt].equals("")){
                return "Attribute must not be NULL";
            }
        }
        Map tablesData = jedis.hgetAll(tableName);
        tablesData.put(entities[0],entities);
        jedis.hmset(tableName,tablesData);

        return "Success!";
    }

    @RequestMapping(value = "/getDatabases", method = RequestMethod.GET)
    public List<String> getDatabases(){
        return this.databases.stream().map(db -> db.getDbName()).collect(Collectors.toList());
    }

    @RequestMapping(value = "/getTables", method = RequestMethod.GET)
    public List<String> getTables(@RequestParam(value="dbName", required = true) String dbName){
        Database database = this.databases.stream().filter(db -> db.getDbName().equalsIgnoreCase(dbName)).findFirst().orElse(null);
        return database.getTables().stream().map(db -> db.getTableName()).collect(Collectors.toList());
    }

    @RequestMapping(value = "/getPrimaryKeys", method = RequestMethod.GET)
    public List<String> getPrimaryKeys(@RequestParam(value="dbName", required = true) String dbName,
                                       @RequestParam(value="tableName", required = true) String tableName){
        Database database = this.databases.stream().filter(db -> db.getDbName().equalsIgnoreCase(dbName)).findFirst().orElse(null);
        Table table = database.getTables().stream().filter(tb -> tb.getTableName().equalsIgnoreCase(tableName)).findFirst().orElse(null);
        return table.getPrimaryKeys();
    }


    @RequestMapping(value = "/delete", method = RequestMethod.POST)
    public String delete(@RequestParam(value="dbName", required = true) String dbName,
                         @RequestParam(value="tableName", required = true) String tableName,
                         @RequestBody String values) throws UnsupportedEncodingException {

        Map map = new HashMap();
        Jedis jedis = new Jedis("localhost");
        Map tablesData4 = jedis.hgetAll("ce");
        //check type, length and IsNull
        //first primary keys; second foreign keys, attributes
        String result = java.net.URLDecoder.decode(values, "UTF-8");
        result = result.substring(1, result.length() - 2);
        String[] entities = result.split("#");



        Database database = this.databases.stream().filter(db -> db.getDbName().equalsIgnoreCase(dbName)).findFirst().orElse(null);
        Table table = database.getTables().stream().filter(tb -> tb.getTableName().equalsIgnoreCase(tableName)).findFirst().orElse(null);


        Object tablesData2 = jedis.hget(tableName,values);
        jedis.hdel(tableName,values);
        Object tablesData3 = jedis.hget(tableName,values);
        return "Success!";
    }
}