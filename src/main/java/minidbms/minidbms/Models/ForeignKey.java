package minidbms.minidbms.Models;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class ForeignKey {
    private String nameForeignKey;
    private String tableReference;
    private String keyReference;

    public ForeignKey(String nameForeignKey, String tableReference, String keyReference){
        this.nameForeignKey = nameForeignKey;
        this.tableReference = tableReference;
        this.keyReference = keyReference;
    }

    public ForeignKey(){
    }
}
