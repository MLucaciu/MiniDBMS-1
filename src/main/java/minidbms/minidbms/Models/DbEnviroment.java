package minidbms.minidbms.Models;

import com.sleepycat.bind.serial.ClassCatalog;
import com.sleepycat.bind.serial.SerialBinding;
import com.sleepycat.bind.serial.StoredClassCatalog;
import com.sleepycat.bind.tuple.TupleBinding;
import com.sleepycat.collections.StoredSortedMap;
import com.sleepycat.collections.TransactionWorker;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.Environment;

import java.util.SortedMap;

public class DbEnviroment implements TransactionWorker {
    private Environment env;
    private ClassCatalog catalog;
    private Database db;
    private SortedMap<String, String> map;

    public DbEnviroment(Environment env, String dbName) throws Exception {
        this.env = env;
        open(dbName);
    }

    /** Opens the database and creates the Map. */
    private void open(String dbName) throws Exception {

        // use a generic database configuration
        DatabaseConfig dbConfig = new DatabaseConfig();
        dbConfig.setTransactional(true);
        dbConfig.setAllowCreate(true);

        // catalog is needed for serial bindings (java serialization)
        Database catalogDb = env.openDatabase(null, "catalog", dbConfig);
        catalog = new StoredClassCatalog(catalogDb);

        // use Integer tuple binding for key entries
        TupleBinding<String> keyBinding =
                TupleBinding.getPrimitiveBinding(String.class);

        // use String serial binding for data entries
        SerialBinding<String> dataBinding =
                new SerialBinding<String>(catalog, String.class);

        this.db = env.openDatabase(null, dbName, dbConfig);

        // create a map view of the database
        this.map = new StoredSortedMap<String, String>(db, keyBinding, dataBinding, true);
    }

    /** Closes the database. */
    public void close() throws Exception {

        if (catalog != null) {
            catalog.close();
            catalog = null;
        }
        if (db != null) {
            db.close();
            db = null;
        }
        if (env != null) {
            env.close();
            env = null;
        }
    }

    @Override
    public void doWork() throws Exception {

    }
}
