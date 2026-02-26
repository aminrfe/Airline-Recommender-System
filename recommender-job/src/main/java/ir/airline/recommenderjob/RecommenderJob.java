package ir.airline.recommenderjob;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
public class RecommenderJob {

    public static void main(String[] args) {
        SpringApplication.run(RecommenderJob.class, args);
    }
}