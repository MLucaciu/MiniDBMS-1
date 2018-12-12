package minidbms.minidbms.Models;

import java.io.Serializable;

/**
 * Created by Mircea on 12/11/2018.
 */
public class Students implements Serializable {
    private int status;
    private String city;
    private String name;

    public Students(String name, int status, String city)
    {
        this.name = name;
        this.status = status;
        this.city = city;
    }

    public final String getName()
    {
        return name;
    }

    public final int getStatus()
    {
        return status;
    }

    public final String getCity()
    {
        return city;
    }

    public String toString()
    {
        return "[Student: name=" + name +
                " status=" + status +
                " city=" + city + ']';
    }
}
