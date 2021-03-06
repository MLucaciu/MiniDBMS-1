package minidbms.minidbms;

import com.sleepycat.collections.TransactionRunner;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.servlet.config.annotation.EnableWebMvc;

import java.io.File;

@SpringBootApplication
public class MinidbmsApplication {

    public static void main(String[] args) throws Exception {

        SpringApplication.run(MinidbmsApplication.class, args);
    }
}
