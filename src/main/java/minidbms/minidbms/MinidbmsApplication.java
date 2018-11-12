package minidbms.minidbms;

import com.sleepycat.collections.TransactionRunner;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;

import java.io.File;

@SpringBootApplication
@ComponentScan({"com.sleepycat.je"})
public class MinidbmsApplication {

    public static void main(String[] args) throws Exception {

        SpringApplication.run(MinidbmsApplication.class, args);

        EnvironmentConfig envConfig = new EnvironmentConfig();
        envConfig.setTransactional(true);
        envConfig.setAllowCreate(true);
        Environment env = new Environment(new File(".\\database"), envConfig);

        // create the application and run a transaction
        DBMSController worker = new DBMSController(env);
        TransactionRunner runner = new TransactionRunner(env);
        try {
            // open and access the database within a transaction
            runner.run(worker);
        } finally {
            // close the database outside the transaction
            worker.close();
        }
    }
}
