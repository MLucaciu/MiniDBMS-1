package minidbms.minidbms;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sleepycat.bind.serial.StoredClassCatalog;
import com.sleepycat.bind.tuple.StringBinding;
import com.sleepycat.collections.TransactionRunner;
import com.sleepycat.collections.TransactionWorker;
import com.sleepycat.je.*;
import com.sleepycat.persist.PrimaryIndex;
import minidbms.minidbms.Models.Attribute;
import minidbms.minidbms.Models.DbEnviroment;
import minidbms.minidbms.Models.IndexFile;
import minidbms.minidbms.Models.Table;
import org.json.JSONException;
import org.springframework.web.bind.annotation.*;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.persist.EntityCursor;

import java.io.*;
import java.lang.reflect.Array;
import java.util.*;
import java.util.stream.Collectors;


@RestController
@CrossOrigin(origins = "*")
public class DBMSController implements TransactionWorker{

    private List<minidbms.minidbms.Models.Database> databases = new ArrayList<>();
    private ObjectMapper mapper = new ObjectMapper();

    private String PATH_TO_JSON = "D:\\Faculty\\minidbms\\database.json";

    private Environment env;
    private StoredClassCatalog javaCatalog;
    EnvironmentConfig envConfig;
    DatabaseConfig dbConfig;
    Cursor cursor;

    String[][] records1 = {{"27", "Jonah"}, {"18", "Alan"}, {"28", "Glory"},
            {"18", "Popeye"}, {"28", "Alan"}};

    String[][] records2 = {{"Jonah", "Whales"}, {"Jonah", "Spiders"},
            {"Alan", "Ghosts"}, {"Alan", "Zombies"}, {"Glory", "Buffy"},
            {"Bob", "foo"}};

    public DBMSController(){
    }

    @RequestMapping(value = "/createDatabase", method = RequestMethod.POST)
    @ResponseBody
    public String createDataBase(@RequestParam(value="dbName", required = true) String dbName) throws IOException {
        if(this.databases.stream().filter(db -> db.getDbName().equals(dbName)).count() != 0){
            return "Database already exists!";
        }
        databases.add(new minidbms.minidbms.Models.Database(dbName));
        mapper.writeValue(new File(PATH_TO_JSON), databases );
        return "Success!";
    }

    @RequestMapping(value = "/addData", method = RequestMethod.POST)
    @ResponseBody
    public String addData() throws Exception {
        createDataBase("Flowers");
        createTable("Flowers", "dbName=Flowers&%7B%22tableName%22%3A%22ExoticCategory%22%2C%22fileName%22%3A%22%22%2C%22rowLength%22%3A%22%22%2C%22structure%22%3A%5B%7B%22attributeName%22%3A%22IdCategory%22%2C%22type%22%3A%22Integer%22%2C%22length%22%3A%225%22%2C%22isNull%22%3A%22False%22%7D%2C%7B%22attributeName%22%3A%22Name%22%2C%22type%22%3A%22String%22%2C%22length%22%3A%2210%22%2C%22isNull%22%3A%22False%22%7D%2C%7B%22attributeName%22%3A%22Country%22%2C%22type%22%3A%22String%22%2C%22length%22%3A%2215%22%2C%22isNull%22%3A%22False%22%7D%5D%2C%22primaryKeys%22%3A%5B%22Id%22%5D%2C%22foreignKeys%22%3A%5B%5D%2C%22indexFiles%22%3A%5B%5D%7D=");
        createTable("Flowers", "dbName=&%7B%22tableName%22%3A%22RosesCategory%22%2C%22fileName%22%3A%22%22%2C%22rowLength%22%3A%22%22%2C%22structure%22%3A%5B%7B%22attributeName%22%3A%22Id%22%2C%22type%22%3A%22Integer%22%2C%22length%22%3A%225%22%2C%22isNull%22%3A%22False%22%7D%2C%7B%22attributeName%22%3A%22Name%22%2C%22type%22%3A%22String%22%2C%22length%22%3A%2210%22%2C%22isNull%22%3A%22False%22%7D%5D%2C%22primaryKeys%22%3A%5B%22Id%22%5D%2C%22foreignKeys%22%3A%5B%5D%2C%22indexFiles%22%3A%5B%5D%7D=");
        createTable("Flowers", "dbName=Flowers&%7B%22tableName%22%3A%22ExoticFlowers%22%2C%22fileName%22%3A%22%22%2C%22rowLength%22%3A%22%22%2C%22structure%22%3A%5B%7B%22attributeName%22%3A%22Id%22%2C%22type%22%3A%22Integer%22%2C%22length%22%3A%2210%22%2C%22isNull%22%3A%22False%22%7D%2C%7B%22attributeName%22%3A%22Name%22%2C%22type%22%3A%22String%22%2C%22length%22%3A%2210%22%2C%22isNull%22%3A%22False%22%7D%5D%2C%22primaryKeys%22%3A%5B%22Id%22%5D%2C%22foreignKeys%22%3A%5B%7B%22tableReference%22%3A%22ExoticCategory%22%2C%22keyReference%22%3A%22IdCategory%22%2C%22nameForeignKey%22%3A%22IdCategory%22%7D%5D%2C%22indexFiles%22%3A%5B%5D%7D=" );
        insert("Flowers", "ExoticCategory", "dbName=Flowers&tableName=ExoticCategory&%221%23Heliconia%23Madagascar%22=");
        insert("Flowers", "ExoticCategory", "dbName=Flowers&tableName=ExoticCategory&%222%23Ginger%23Australia%22=");
        insert("Flowers", "ExoticFlowers", "dbName=Flowers&tableName=ExoticFlowers&%221%23Lobster%231%22=");
        insert("Flowers", "ExoticFlowers", "dbName=Flowers&tableName=ExoticFlowers&%222%23Anthurium%231%22=");
        insert("Flowers", "ExoticFlowers", "dbName=Flowers&tableName=ExoticFlowers&%223%23Mokara%231%22=");
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
                              @RequestBody(required = false) String indexFile) throws Exception {


        PrimaryIndex<String,IndexFile> indexFileIndex;

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
            //create file for new table
            // environment is transactional
            EnvironmentConfig envConfig = new EnvironmentConfig();
            envConfig.setTransactional(true);
            envConfig.setAllowCreate(true);
            String fileName = database.getDbName() + "-" + tableName + "Index";
            Environment env = new Environment(new File("."), envConfig);
            //table.addindexFile(newIndexFile);
            //map.put(tableName + "Index",result);
            mapper.writeValue(new File(PATH_TO_JSON), databases );
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
        return "Index file successfully created!";
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
        DatabaseEntry keyEntry;
        DatabaseEntry dataEntry;

        for(int i =0; i<table.getForeignKeys().size();i++){
            envConfig = new EnvironmentConfig();
            envConfig.setTransactional(true);
            envConfig.setAllowCreate(true);
            env = new Environment(new File("."), envConfig);

            // Set the Berkeley DB config for opening all stores.
            dbConfig = new DatabaseConfig();
            dbConfig.setTransactional(true);
            dbConfig.setAllowCreate(true);

            // Create the Serial class catalog.  This holds the serialized class format for all database records of serial format.
            com.sleepycat.je.Database catalogDb = env.openDatabase(null, dbName + "-" + table.getForeignKeys().get(i).getTableReference(), dbConfig);
            javaCatalog = new StoredClassCatalog(catalogDb);

            cursor = catalogDb.openCursor(null, null);
            keyEntry = new DatabaseEntry();
            dataEntry = new DatabaseEntry();

            boolean foreignKeyFound = false;
            while (cursor.getNext(keyEntry, dataEntry, LockMode.DEFAULT) ==
                    OperationStatus.SUCCESS) {
                if(entities[entities.length-table.getForeignKeys().size()+i].equalsIgnoreCase(StringBinding.entryToString(keyEntry))){
                    foreignKeyFound = true;
                }
            }

            if(!foreignKeyFound){
                return "Foreign Key does not exists!";
            }
            cursor.close();
        }

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

        envConfig = new EnvironmentConfig();
        envConfig.setTransactional(true);
        envConfig.setAllowCreate(true);
        env = new Environment(new File("."), envConfig);

        // Set the Berkeley DB config for opening all stores.
        dbConfig = new DatabaseConfig();
        dbConfig.setTransactional(true);
        dbConfig.setAllowCreate(true);

        // Create the Serial class catalog.  This holds the serialized class
        // format for all database records of serial format.
        com.sleepycat.je.Database catalogDb = env.openDatabase(null, dbName + "-" + tableName, dbConfig);
        javaCatalog = new StoredClassCatalog(catalogDb);

        Transaction txn = env.beginTransaction(null, null);

        keyEntry = new DatabaseEntry();
        dataEntry = new DatabaseEntry();

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

    /**
     *
     * @param tableName
     * @param dbName
     * @param columns
     * @param condition
     * @return
     * @throws IOException
     */
    @RequestMapping(value = "/select", method = RequestMethod.GET)
    public String select(@RequestParam(value="dbName", required = true) String dbName,
                            @RequestParam(value="tableName", required = true) String tableName,
                            @RequestParam(value="columns", required = true) String columns,
                             @RequestParam(value="condition", required = true) String condition)
    throws IOException {

        minidbms.minidbms.Models.Database database = this.databases.stream().filter(db -> db.getDbName().equalsIgnoreCase(dbName)).findFirst().orElse(null);
        Table table = database.getTables().stream().filter(tb -> tb.getTableName().equalsIgnoreCase(tableName)).findFirst().orElse(null);

        String[] cols = columns.split(",");
//        for (String col : cols) {
//            System.out.println(col);
//        }

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

//        DatabaseEntry keyEntry = new DatabaseEntry();
//        DatabaseEntry dataEntry = new DatabaseEntry();
//
//        StringBinding.stringToEntry(tableName, keyEntry);
//        StringBinding.stringToEntry(valuesEntity, dataEntry);
//
//        OperationStatus status = catalogDb.get(txn, keyEntry, dataEntry,LockMode.READ_COMMITTED);
//        txn.commit();

//        /* retrieve the data */
        Cursor cursor = catalogDb.openCursor(null, null);
//
//        while (cursor.getNext(keyEntry, dataEntry, LockMode.DEFAULT) ==
//                OperationStatus.SUCCESS) {
//            System.out.println("key=" +
//                    StringBinding.entryToString(keyEntry) +
//                    " data=" +
//                    StringBinding.entryToString(dataEntry));
//        }
//        cursor.close();

//TODO

        DatabaseEntry foundKey = new DatabaseEntry();
        DatabaseEntry foundData = new DatabaseEntry();
        String[] cond = condition.split("=");
        String res = "";
        while (cursor.getNext(foundKey, foundData, LockMode.DEFAULT) ==
                OperationStatus.SUCCESS) {

        String cddeva =  foundKey.toString();
String altccceva = foundData.toString();
            String keyString = new String(foundKey.getData(),"UTF-8");
            String dataString = new String(foundData.getData(),"UTF-8");
            //TODO : contains is always false, because of non-UTF8 characters
            boolean contains = Arrays.stream(cols).anyMatch(keyString::equals);
            if (contains && keyString.equals(cond[0]) && dataString.equals(cond[1])) {
                res = res + keyString + " " + dataString + '\n';
            }
           // res = res + keyString + " " + dataString + '\n';
//            System.out.println("Key | Data : " + keyString + " | " +
//                    dataString + "");
        }
        cursor.close();
        return res;
    }

    /**
     * Work in progress
     * @param digest
     * @return
     */
    private static String toHexadecimal(byte[] digest){
        String hash = "";
        for(byte aux : digest) {
            int b = aux & 0xff;
            if (Integer.toHexString(b).length() == 1) hash += "0";
            hash += Integer.toHexString(b);
        }
        return hash;
    }

    @RequestMapping(value = "/selectHashJoin", method = RequestMethod.GET)
    public String selectHashJoin(@RequestParam(value="dbName", required = true) String dbName,
                         @RequestParam(value="tableName", required = true) String tableName,
                         @RequestParam(value="tableNameJoin", required = true) String tableNameJoin,
                         @RequestParam(value = "joinColumn", required = true) String joinColumn,
                         @RequestParam(value="columns", required = true) String columns,
                         @RequestParam(value="condition", required = true) String condition) {


        DatabaseEntry keyEntry, dataEntry;
        String result = "";
        Map<String, String> hashMapTable = new HashMap<>();

        minidbms.minidbms.Models.Database database = this.databases.stream().filter(db -> db.getDbName().equalsIgnoreCase(dbName)).findFirst().orElse(null);
        Table table = database.getTables().stream().filter(tb -> tb.getTableName().equalsIgnoreCase(tableName)).findFirst().orElse(null);
        Table tableJoin = database.getTables().stream().filter(tb -> tb.getTableName().equalsIgnoreCase(tableNameJoin)).findFirst().orElse(null);

        envConfig = new EnvironmentConfig();
        envConfig.setTransactional(true);
        envConfig.setAllowCreate(true);
        env = new Environment(new File("."), envConfig);

        // Set the Berkeley DB config for opening all stores.
        dbConfig = new DatabaseConfig();
        dbConfig.setTransactional(true);
        dbConfig.setAllowCreate(true);

        // Create the Serial class catalog.  This holds the serialized class format for all database records of serial format.
        com.sleepycat.je.Database catalogDb = env.openDatabase(null, dbName + "-" + tableName, dbConfig);
        com.sleepycat.je.Database catalogDbJoin = env.openDatabase(null, dbName + "-" + tableNameJoin, dbConfig);

        Integer indexForeignKey;
        Integer indexAttribute;
        Integer noForeignKeys;
        Integer noForeignKeysJoin;
        Integer indexForeignKeyJoin;
        Integer indexAttributeJoin;
        Cursor cursorJoin;

        if(catalogDb.count() < catalogDbJoin.count()){
            cursor = catalogDb.openCursor(null, null);
            cursorJoin = catalogDbJoin.openCursor(null, null);
            indexForeignKey = table.indexOfForeignKey(joinColumn);
            indexAttribute = table.indexOfAttribute(joinColumn);
            noForeignKeys = table.getForeignKeys().size();
            indexForeignKeyJoin = tableJoin.indexOfForeignKey(joinColumn);
            indexAttributeJoin =tableJoin.indexOfAttribute(joinColumn);
            noForeignKeysJoin = tableJoin.getForeignKeys().size();
        } else {
            cursor = catalogDbJoin.openCursor(null, null);
            cursorJoin = catalogDb.openCursor(null, null);
            indexForeignKey = tableJoin.indexOfForeignKey(joinColumn);
            indexAttribute = tableJoin.indexOfAttribute(joinColumn);
            noForeignKeys = tableJoin.getForeignKeys().size();
            indexForeignKeyJoin = table.indexOfForeignKey(joinColumn);
            indexAttributeJoin =table.indexOfAttribute(joinColumn);
            noForeignKeysJoin = table.getForeignKeys().size();
        }

        keyEntry = new DatabaseEntry();
        dataEntry = new DatabaseEntry();

        while (cursor.getNext(keyEntry, dataEntry, LockMode.DEFAULT) ==
                OperationStatus.SUCCESS) {
            if (indexAttribute != -1){
                if (indexAttribute == 0){
                    hashMapTable.put(StringBinding.entryToString(keyEntry), StringBinding.entryToString(keyEntry) + '#' + StringBinding.entryToString(dataEntry));
                } else {
                    String[] entities = StringBinding.entryToString(dataEntry).split("#");
                    hashMapTable.put(entities[indexAttribute-1], StringBinding.entryToString(keyEntry) + '#' + StringBinding.entryToString(dataEntry));
                }
            }

            if(indexForeignKey != -1){
                String[] entities = StringBinding.entryToString(dataEntry).split("#");
                hashMapTable.put(entities[entities.length  - noForeignKeys], StringBinding.entryToString(keyEntry) + '#' + StringBinding.entryToString(dataEntry));
            }
        }

        while (cursorJoin.getNext(keyEntry, dataEntry, LockMode.DEFAULT) ==
                OperationStatus.SUCCESS) {
            if(indexAttributeJoin != -1){
                if(indexAttributeJoin == 0 ){
                    hashMapTable.put(StringBinding.entryToString(keyEntry),  hashMapTable.get(StringBinding.entryToString(keyEntry))+ ">>>" + StringBinding.entryToString(keyEntry) + '#' + StringBinding.entryToString(dataEntry));
                } else {
                    String[] entities = StringBinding.entryToString(dataEntry).split("#");
                    hashMapTable.put(entities[indexAttributeJoin-1], hashMapTable.get(entities[indexAttributeJoin-1]) + ">>>" + StringBinding.entryToString(keyEntry) + '#' + StringBinding.entryToString(dataEntry));
                }
            }

            if(indexForeignKeyJoin != -1){
                String[] entities = StringBinding.entryToString(dataEntry).split("#");
                hashMapTable.put(entities[entities.length - noForeignKeysJoin], hashMapTable.get(entities[entities.length - noForeignKeysJoin]) + ">>>" + StringBinding.entryToString(keyEntry) + "#" + StringBinding.entryToString(dataEntry));
            }
        }

        hashMapTable.remove("");
        return hashMapTable.toString();

    }

    @RequestMapping(value = "/selectNestedLoopJoin", method = RequestMethod.GET)
    public ArrayList<String> selectNestedLoopJoin(@RequestParam(value="dbName", required = true) String dbName,
                                 @RequestParam(value="tableName", required = true) String tableName,
                                 @RequestParam(value="tableNameJoin", required = true) String tableNameJoin,
                                 @RequestParam(value = "joinColumn", required = true) String joinColumn,
                                 @RequestParam(value="columns", required = true) String columns,
                                 @RequestParam(value="condition", required = true) String condition) {

        minidbms.minidbms.Models.Database database = this.databases.stream().filter(db -> db.getDbName().equalsIgnoreCase(dbName)).findFirst().orElse(null);
        Table table = database.getTables().stream().filter(tb -> tb.getTableName().equalsIgnoreCase(tableName)).findFirst().orElse(null);
        Table tableJoin = database.getTables().stream().filter(tb -> tb.getTableName().equalsIgnoreCase(tableNameJoin)).findFirst().orElse(null);

        DatabaseEntry keyEntry, dataEntry, keyEntryJoin, dataEntryJoin;
        ArrayList<String> result = new ArrayList<>();

        //check if index exists!

        envConfig = new EnvironmentConfig();
        envConfig.setTransactional(true);
        envConfig.setAllowCreate(true);
        env = new Environment(new File("."), envConfig);

        // Set the Berkeley DB config for opening all stores.
        dbConfig = new DatabaseConfig();
        dbConfig.setTransactional(true);
        dbConfig.setAllowCreate(true);

        // Create the Serial class catalog.  This holds the serialized class format for all database records of serial format.
        com.sleepycat.je.Database catalogDb = env.openDatabase(null, dbName + "-" + tableName, dbConfig);
        com.sleepycat.je.Database catalogDbJoin = env.openDatabase(null, dbName + "-" + tableNameJoin, dbConfig);

        cursor = catalogDb.openCursor(null, null);
        Cursor cursorJoin = catalogDbJoin.openCursor(null, null);

        keyEntry = new DatabaseEntry();
        dataEntry = new DatabaseEntry();
        keyEntryJoin = new DatabaseEntry();
        dataEntryJoin = new DatabaseEntry();

        Integer indexForeignKey = table.indexOfForeignKey(joinColumn);
        Integer indexAttribute = table.indexOfAttribute(joinColumn);
        Integer noForeignKeys = table.getForeignKeys().size();
        Integer indexForeignKeyJoin = tableJoin.indexOfForeignKey(joinColumn);
        Integer indexAttributeJoin =tableJoin.indexOfAttribute(joinColumn);
        Integer noForeignKeysJoin = tableJoin.getForeignKeys().size();

        while (cursor.getNext(keyEntry, dataEntry, LockMode.DEFAULT) ==
                OperationStatus.SUCCESS) {
            if(!StringBinding.entryToString(keyEntry).isEmpty()){
                String onAtt = "";
                if(indexAttribute != -1){
                    if(indexAttribute == 0 ){
                        onAtt = StringBinding.entryToString(keyEntry);
                    } else {
                        String[] entities = StringBinding.entryToString(dataEntry).split("#");
                        onAtt = entities[indexAttribute-1];
                    }
                }
                if(indexForeignKey != -1){
                    String[] entities = StringBinding.entryToString(dataEntry).split("#");
                    onAtt = entities[entities.length - noForeignKeys];
                }

                while (cursorJoin.getNext(keyEntryJoin, dataEntryJoin, LockMode.DEFAULT) ==
                        OperationStatus.SUCCESS) {
                    if(!StringBinding.entryToString(keyEntryJoin).isEmpty()){
                        String onAttJoin ="";
                        if(indexAttributeJoin != -1){
                            if(indexAttributeJoin == 0 ){
                                onAttJoin = StringBinding.entryToString(keyEntryJoin);
                            } else {
                                String[] entities = StringBinding.entryToString(dataEntryJoin).split("#");
                                onAttJoin = entities[indexAttributeJoin-1];
                            }
                        }
                        if(indexForeignKeyJoin != -1){
                            String[] entities = StringBinding.entryToString(dataEntryJoin).split("#");
                            onAttJoin = entities[entities.length - noForeignKeysJoin];
                        }

                        if(onAtt.equalsIgnoreCase(onAttJoin)){
                            result.add(StringBinding.entryToString(keyEntry) + "#" + StringBinding.entryToString(dataEntry) + ">>>" + StringBinding.entryToString(keyEntryJoin) + "#" + StringBinding.entryToString(dataEntryJoin));
                        }
                    }
                }
            }
        }

        cursorJoin.close();
        cursor.close();

        return result;
    }
}