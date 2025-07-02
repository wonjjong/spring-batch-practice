package com.example.batch.config;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.test.JobLauncherTestUtils;
import org.springframework.batch.test.context.SpringBatchTest;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.ActiveProfiles;

import com.example.batch.TestConfig;
import com.example.batch.service.TestDataGeneratorService;

import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.BatchStatus;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@SpringBootTest
@SpringBatchTest
@Import(TestConfig.class)
@ActiveProfiles("test")
@Slf4j
public class PartnerAggregationBatchConfigTest {
    @Autowired
    private JobLauncher jobLauncher;
    @Autowired
    private TestDataGeneratorService testDataGeneratorService;
    @Autowired
    private JobLauncherTestUtils jobLauncherTestUtils;

    @Autowired
    private Job partnerAggregationJob;


     @BeforeEach
     void setUp() {
         // 테스트 데이터 생성 (1만건)
         int testCount = 100_00;
         log.info("테스트 데이터 생성 시작: {} 건", testCount);
        
         long startTime = System.currentTimeMillis();
         testDataGeneratorService.generateJdbcTestData(testCount);
         long endTime = System.currentTimeMillis();
        
         log.info("테스트 데이터 생성 완료: {} 초", (endTime - startTime) / 1000.0);

         jobLauncherTestUtils.setJob(partnerAggregationJob);
     }

    @Test
    void partnerAggregationJob_정상수행_테스트() throws Exception {
        JobParameters jobParameters = new JobParametersBuilder()
                .addLong("time", System.currentTimeMillis())
                .toJobParameters();

        log.info("=== Job 시작 ===");
        JobExecution jobExecution = jobLauncherTestUtils.launchJob(jobParameters);
        log.info("=== Job 종료 ===");

        assertEquals(BatchStatus.COMPLETED, jobExecution.getStatus());
        log.info("JobExecution Status: {}", jobExecution.getStatus());
    }
}
