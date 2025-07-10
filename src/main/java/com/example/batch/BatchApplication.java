package com.example.batch;

import com.example.batch.service.TestDataGeneratorService;
import lombok.RequiredArgsConstructor;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.stereotype.Component;

@SpringBootApplication
public class BatchApplication {
    public static void main(String[] args) {
        SpringApplication.run(BatchApplication.class, args);
    }

    @Component
    @RequiredArgsConstructor
    public static class Datainitializer implements ApplicationRunner {
        private final TestDataGeneratorService testDataGeneratorService;

        @Override
        public void run(ApplicationArguments args) {
            testDataGeneratorService.generateJdbcTestData(10000);
        }
    }
}
