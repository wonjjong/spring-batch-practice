package com.example.batch;


import com.example.batch.service.TestDataGeneratorService;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.test.JobLauncherTestUtils;
import org.springframework.batch.test.context.SpringBatchTest;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;

import static org.junit.jupiter.api.Assertions.assertEquals;

@Slf4j
@SpringBootTest
@SpringBatchTest
@Import(TestConfig.class)
public class BatchRetryTest {
    @Autowired
    private TestDataGeneratorService testDataGeneratorService;
    @Autowired
    private JobLauncherTestUtils jobLauncherTestUtils;
    @Autowired
    private Job exceptionRetryJob;

    @BeforeEach
    void setUp() {
        // 테스트 데이터 생성 (1만건)
        int testCount = 100_00;
        log.info("테스트 데이터 생성 시작: {} 건", testCount);

        long startTime = System.currentTimeMillis();
        testDataGeneratorService.generateJdbcTestData(testCount);
        long endTime = System.currentTimeMillis();

        log.info("테스트 데이터 생성 완료: {} 초", (endTime - startTime) / 1000.0);
    }

    @Test
    @DisplayName("배치 재시도 테스트")
    public void 배치_재시도_테스트() throws Exception {
        //given
        jobLauncherTestUtils.setJob(exceptionRetryJob);
        JobExecution jobExecution = jobLauncherTestUtils.launchJob();

        //when


        //then
        assertEquals(BatchStatus.FAILED, jobExecution.getStatus());
    }


}
