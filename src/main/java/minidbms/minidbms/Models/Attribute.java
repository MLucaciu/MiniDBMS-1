package minidbms.minidbms.Models;

import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;

@Getter
@Setter
public class Attribute implements Serializable {
    private String attributeName;
    private String type;
    private String length;
    private String isNull;

    public Attribute(String attributeName, String type, String length, String isNull){
        this.attributeName = attributeName;
        this.type = type;
        this.length = length;
        this.isNull = isNull;
    }

    public Attribute(){}
}
