package minidbms.minidbms.Models;

import com.sleepycat.je.DatabaseException;
import com.sleepycat.persist.EntityStore;
import com.sleepycat.persist.PrimaryIndex;

/**
 * Created by Mircea on 11/13/2018.
 */
public class IndexAccesor {

    /* Person accessors */
    public PrimaryIndex<String,IndexFile> indexFileIndex;


    /* Opens all primary and secondary indices. */
    public IndexAccesor(EntityStore store)
            throws DatabaseException {

        indexFileIndex = store.getPrimaryIndex(String.class, IndexFile.class);
    }
}