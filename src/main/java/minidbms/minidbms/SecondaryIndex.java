package minidbms.minidbms;

import com.sleepycat.bind.serial.ClassCatalog;
import com.sleepycat.bind.serial.SerialSerialKeyCreator;
import minidbms.minidbms.Models.Students;

/**
 * Created by Mircea on 12/11/2018.
 */
public class SecondaryIndex extends SerialSerialKeyCreator {

        private SecondaryIndex(ClassCatalog catalog,
                                         Class primaryKeyClass,
                                         Class valueClass,
                                         Class indexKeyClass)
        {
            super(catalog, primaryKeyClass, valueClass, indexKeyClass);
        }

        public Object createSecondaryKey(Object primaryKeyInput,
                                         Object valueInput)
        {
            Students stud = (Students) valueInput;
            return stud.getCity();
        }
}
