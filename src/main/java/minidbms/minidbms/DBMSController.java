package minidbms.minidbms;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sleepycat.bind.serial.StoredClassCatalog;
import com.sleepycat.bind.tuple.IntegerBinding;
import com.sleepycat.bind.tuple.StringBinding;
import com.sleepycat.bind.tuple.TupleBinding;
import com.sleepycat.collections.TransactionRunner;
import com.sleepycat.collections.TransactionWorker;
import com.sleepycat.je.*;
import com.sleepycat.persist.EntityStore;
import com.sleepycat.persist.PrimaryIndex;
import com.sleepycat.persist.StoreConfig;
import minidbms.minidbms.Models.*;
import minidbms.minidbms.Models.Database;
import org.json.JSONException;
import org.springframework.web.bind.annotation.*;
import redis.clients.jedis.Jedis;

import java.io.File;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;


@RestController
@CrossOrigin(origins = "*")
public class DBMSController implements TransactionWorker{

    private List<minidbms.minidbms.Models.Database> databases = new ArrayList<>();
    private ObjectMapper mapper = new ObjectMapper();

    private String PATH_TO_JSON = "D:\\Faculty\\minidbms\\database.json";

    private Environment env;
    private StoredClassCatalog javaCatalog;

    public DBMSController(){
    }

    @RequestMapping(value = "/createDatabase", method = RequestMethod.POST)
    @ResponseBody
    public String createDataBase(@RequestParam(value="dbName", required = true) String dbName) throws IOException, JSONException {
        if(this.databases.stream().filter(db -> db.getDbName().equals(dbName)).count() != 0){
            return "Database already exists!";
        }
        databases.add(new minidbms.minidbms.Models.Database(dbName));
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
                              @RequestBody(required = false) String table) throws Exception {
        String result;
        Table newTable;
        //Jedis jedis = new Jedis("localhost");
        //Map tablesData4 = jedis.hgetAll("ce");
        try {
            result = java.net.URLDecoder.decode(table.substring(table.indexOf("&")+1), "UTF-8");
            result = result.substring(0, result.length() - 1);
            newTable = new ObjectMapper().readValue(result, Table.class);
            minidbms.minidbms.Models.Database database = this.databases.stream().filter(db -> db.getDbName().equalsIgnoreCase(dbName)).findFirst().orElse(null);
            if(database.getTables().stream().filter(tb -> tb.getTableName().equals(newTable.getTableName())).count() != 0){
                return "Table already exists!";
            }
            database.addTable(newTable);
            mapper.writeValue(new File(PATH_TO_JSON), databases );
            HashMap hm = new HashMap <String,String>();
            hm.put("database",database.getDbName());

            //create file for new table
            // environment is transactional
            EnvironmentConfig envConfig = new EnvironmentConfig();
            envConfig.setTransactional(true);
            envConfig.setAllowCreate(true);
            String fileName = database.getDbName() + "-" + newTable.getTableName();
            Environment env = new Environment(new File("."), envConfig);

            // create the application and run a transaction
            DbEnviroment worker = new DbEnviroment(env, fileName);
            TransactionRunner runner = new TransactionRunner(env);
            try {
                // open and access the database within a transaction
                runner.run(worker);
            } finally {
                // close the database outside the transaction
                worker.close();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return "Success!";
    }

    @RequestMapping(value = "/dropDatabase", method = RequestMethod.DELETE)
    public String dropDatabase(@RequestParam(value="dbName", required = true) String dbName) throws IOException {
        minidbms.minidbms.Models.Database database = this.databases.stream().filter(db -> db.getDbName().equalsIgnoreCase(dbName)).findFirst().orElse(null);

        if(database != null){
            this.databases.remove(database);
            mapper.writeValue(new File(PATH_TO_JSON), databases );
        }

        return "Success";
    }

    @RequestMapping(value = "/dropTable", method = RequestMethod.DELETE)
    public String dropTable(@RequestParam(value="tableName", required = true) String tableName,
                            @RequestParam(value="dbName", required = true) String dbName) throws IOException {
        minidbms.minidbms.Models.Database database = this.databases.stream().filter(db -> db.getDbName().equalsIgnoreCase(dbName)).findFirst().orElse(null);

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

        ArrayList<String> indexAttributes = new ArrayList<String>();
        indexAttributes.add("ceva");
        PrimaryIndex<String,IndexFile> indexFileIndex;

        EnvironmentConfig envConfig = new EnvironmentConfig();
        envConfig.setAllowCreate(true);
        envConfig.setTransactional(true);
        env = new Environment(new File("."), envConfig);

        StoreConfig storeConfig = new StoreConfig();
        storeConfig.setAllowCreate(true);
        storeConfig.setTransactional(true);

        EntityStore store = new EntityStore(env, tableName + "Index", storeConfig);
        IndexAccesor ia = new IndexAccesor(store);
        ia.indexFileIndex.put
                (new IndexFile(tableName + "Index", "30", "1", "primary", indexAttributes));
        String result;
        IndexFile newIndexFile;
        try {
            result = java.net.URLDecoder.decode(indexFile.substring(indexFile.lastIndexOf("&")+1), "UTF-8");
            result = result.substring(0, result.length() - 1);
            newIndexFile = new ObjectMapper().readValue(result, IndexFile.class);
            minidbms.minidbms.Models.Database database = this.databases.stream().filter(db -> db.getDbName().equalsIgnoreCase(dbName)).findFirst().orElse(null);
            //Table table = database.getTables().stream().filter(tb -> tb.getTableName().equalsIgnoreCase(tableName)).findFirst().orElse(null);
//            if(table.getIndexFiles().stream().filter(ind -> ind.getIndexName().equals(newIndexFile.getIndexName())).count() != 0){
//                return "Index File already exists!";
//            }
            //table.addindexFile(newIndexFile);
            //map.put(tableName + "Index",result);
            mapper.writeValue(new File(PATH_TO_JSON), database );
        } catch (IOException e) {
            e.printStackTrace();
        }
        return "Success!";
    }

    @RequestMapping(value = "/insert", method = RequestMethod.POST)
    public String insert(@RequestParam(value="dbName", required = true) String dbName,
                         @RequestParam(value="tableName", required = true) String tableName,
                         @RequestBody String values) throws UnsupportedEncodingException {

        //check type, length and IsNull
        //first primary keys; second foreign keys, attributes
        String result = java.net.URLDecoder.decode(values.substring(values.lastIndexOf("&")+1), "UTF-8");
        result = result.substring(1, result.length() - 2);
        String[] entities = result.split("#");
        //Jedis jedis = new Jedis("localhost");
        //Map tablesData4 = jedis.hgetAll("ce");

        minidbms.minidbms.Models.Database database = this.databases.stream().filter(db -> db.getDbName().equalsIgnoreCase(dbName)).findFirst().orElse(null);
        Table table = database.getTables().stream().filter(tb -> tb.getTableName().equalsIgnoreCase(tableName)).findFirst().orElse(null);

        //Map tablesData = jedis.hgetAll(tableName);

        //check if the primary key is unique
        int noPrimaryKeys = table.getPrimaryKeys().size();
        for(int i = 0; i < noPrimaryKeys ; i++){
            String primaryKey = entities[i];
            //split("#")
            /*if(((HashMap) tablesData).keySet().stream().filter(e -> e.toString().contains(primaryKey)).count() != 0){
                return "Primary key is not unique";
            }*/
        }

        //check for foreign keys
        /* int noForeignKeys = table.getForeignKeys().size();
        int indexForeignKeys = -1;
        for(int i = noPrimaryKeys; i < noForeignKeys ; i++){
            indexForeignKeys++;
            //split("#")
            String tableForeignKeysName = table.getForeignKeys().get(indexForeignKeys).getTableReference();
            Map tableForeignKeys = jedis.hgetAll(tableForeignKeysName);
            String foreignKeyValue = entities[i];
            if(((HashMap) tableForeignKeys).keySet().stream().filter(e -> e.toString().contains(foreignKeyValue)).count() == 0){
                return "Foreign Key does not exists!";
            }
            if(((HashMap) tableForeignKeys).values().stream().filter(e -> e.toString().contains(foreignKeyValue)).count() == 0){
                return "Foreign Key does not exists!";
            }
        } */

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

        String primaryKey = entities[0];
        for(int i = 1; i < noPrimaryKeys; i++){
            primaryKey += "#" + entities[i];
        }
        String valuesEntity = entities[noPrimaryKeys];
        for(int i = noPrimaryKeys+1; i<entities.length; i++){
            valuesEntity += "#" + entities[i];
        }

        EnvironmentConfig envConfig = new EnvironmentConfig();
        envConfig.setTransactional(true);
        envConfig.setAllowCreate(true);
        env = new Environment(new File("."), envConfig);

        // Set the Berkeley DB config for opening all stores.
        DatabaseConfig dbConfig = new DatabaseConfig();
        dbConfig.setTransactional(true);
        dbConfig.setAllowCreate(true);

        // Create the Serial class catalog.  This holds the serialized class
        // format for all database records of serial format.
        //
        com.sleepycat.je.Database catalogDb = env.openDatabase(null, dbName + "-" + tableName, dbConfig);
        javaCatalog = new StoredClassCatalog(catalogDb);

        Transaction txn = env.beginTransaction(null, null);

        DatabaseEntry keyEntry = new DatabaseEntry();
        DatabaseEntry dataEntry = new DatabaseEntry();

        StringBinding.stringToEntry(primaryKey, keyEntry);
        StringBinding.stringToEntry(valuesEntity, dataEntry);

        OperationStatus status = catalogDb.put(txn, keyEntry, dataEntry);

        txn.commit();

        /* retrieve the data */
        Cursor cursor = catalogDb.openCursor(null, null);

        while (cursor.getNext(keyEntry, dataEntry, LockMode.DEFAULT) ==
                OperationStatus.SUCCESS) {
            System.out.println("key=" +
                    StringBinding.entryToString(keyEntry) +
                    " data=" +
                    StringBinding.entryToString(dataEntry));
        }
        cursor.close();

        //map.put(primaryKey,valuesEntity);

        return "Success!";
    }

    @RequestMapping(value = "/getDatabases", method = RequestMethod.GET)
    public List<String> getDatabases(){
        return this.databases.stream().map(db -> db.getDbName()).collect(Collectors.toList());
    }

    @RequestMapping(value = "/getTables", method = RequestMethod.GET)
    public List<String> getTables(@RequestParam(value="dbName", required = true) String dbName){
        minidbms.minidbms.Models.Database database = this.databases.stream().filter(db -> db.getDbName().equalsIgnoreCase(dbName)).findFirst().orElse(null);
        return database.getTables().stream().map(db -> db.getTableName()).collect(Collectors.toList());
    }

    @RequestMapping(value = "/getPrimaryKeys", method = RequestMethod.GET)
    public List<String> getPrimaryKeys(@RequestParam(value="dbName", required = true) String dbName,
                                       @RequestParam(value="tableName", required = true) String tableName){
        minidbms.minidbms.Models.Database database = this.databases.stream().filter(db -> db.getDbName().equalsIgnoreCase(dbName)).findFirst().orElse(null);
        Table table = database.getTables().stream().filter(tb -> tb.getTableName().equalsIgnoreCase(tableName)).findFirst().orElse(null);
        return table.getPrimaryKeys();
    }

    @RequestMapping(value = "/getTable", method = RequestMethod.GET)
    public Table getTable(@RequestParam(value="dbName", required = true) String dbName,
                          @RequestParam(value="tableName", required = true) String tableName){
        minidbms.minidbms.Models.Database database = this.databases.stream().filter(db -> db.getDbName().equalsIgnoreCase(dbName)).findFirst().orElse(null);
        Table table = database.getTables().stream().filter(tb -> tb.getTableName().equalsIgnoreCase(tableName)).findFirst().orElse(null);
        return table;
    }

    @RequestMapping(value = "/delete", method = RequestMethod.POST)
    public String delete(@RequestParam(value="dbName", required = true) String dbName,
                         @RequestParam(value="tableName", required = true) String tableName,
                         @RequestBody String values) throws UnsupportedEncodingException {

        Map map = new HashMap();
        //Jedis jedis = new Jedis("localhost");
        //Map tablesData4 = jedis.hgetAll("ce");
        //check type, length and IsNull
        //first primary keys; second foreign keys, attributes
        String result = java.net.URLDecoder.decode(values.substring(values.lastIndexOf("&")+1), "UTF-8");
        result = result.substring(1, result.length() - 2);
        String[] entities = result.split("#");

        EnvironmentConfig envConfig = new EnvironmentConfig();
        envConfig.setTransactional(true);
        envConfig.setAllowCreate(true);
        env = new Environment(new File("."), envConfig);

        // Set the Berkeley DB config for opening all stores.
        DatabaseConfig dbConfig = new DatabaseConfig();
        dbConfig.setTransactional(true);
        dbConfig.setAllowCreate(true);

        // Create the Serial class catalog.  This holds the serialized class
        // format for all database records of serial format.
        //
        com.sleepycat.je.Database catalogDb = env.openDatabase(null, dbName + "-" + tableName, dbConfig);
        javaCatalog = new StoredClassCatalog(catalogDb);

        Transaction txn = env.beginTransaction(null, null);

        DatabaseEntry keyEntry = new DatabaseEntry();
        DatabaseEntry dataEntry = new DatabaseEntry();

        StringBinding.stringToEntry(result, keyEntry);

        OperationStatus status = catalogDb.delete(txn, keyEntry);

        txn.commit();

        /* retrieve the data */
        Cursor cursor = catalogDb.openCursor(null, null);

        while (cursor.getNext(keyEntry, dataEntry, LockMode.DEFAULT) ==
                OperationStatus.SUCCESS) {
            System.out.println("key=" +
                    StringBinding.entryToString(keyEntry) +
                    " data=" +
                    StringBinding.entryToString(dataEntry));
        }
        cursor.close();

        //Object tablesData2 = jedis.hget(tableName,values);
        //jedis.hdel(tableName,values);
        //Object tablesData3 = jedis.hget(tableName,values);
        return "Success!";
    }

    @Override
    public void doWork() throws Exception {
    }
}