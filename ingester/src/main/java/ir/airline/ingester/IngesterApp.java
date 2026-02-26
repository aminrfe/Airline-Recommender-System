package ir.airline.ingester;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
public class IngesterApp {

    public static void main(String[] args) {
        SpringApplication.run(IngesterApp.class, args);
    }
}

