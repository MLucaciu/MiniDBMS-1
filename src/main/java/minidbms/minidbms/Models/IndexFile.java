package minidbms.minidbms.Models;

import com.sleepycat.persist.model.Entity;
import com.sleepycat.persist.model.PrimaryKey;
import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;
import java.util.List;

@Getter
@Setter
@Entity
public class IndexFile implements Serializable {
    @PrimaryKey
    private String indexName;
    private String keyLength;
    private String isUnique;
    private String indexType;
    private List<String> indexAttributes;

    public IndexFile(String indexName, String keyLength, String isUnique, String indexType, List<String> indexAttributes){
        this.indexName = indexName;
        this.keyLength = keyLength;
        this.isUnique = isUnique;
        this.indexType = indexType;
        this.indexAttributes = indexAttributes;
    }

    public IndexFile(){}

    public String getIndexName() {
        return indexName;
    }
}
