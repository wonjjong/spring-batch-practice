package com.example.batch;

import com.example.batch.repository.PlayerUserLogRepository;
import com.example.batch.service.TestDataGeneratorService;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.batch.test.JobLauncherTestUtils;
import org.springframework.batch.test.context.SpringBatchTest;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;

@SpringBootTest
@Import(TestConfig.class)
@SpringBatchTest
@Slf4j
public class PlayerUserlogBatchTest {
    @Autowired
    private JobLauncherTestUtils jobLauncherTestUtils;

    @Autowired
    private TestDataGeneratorService testDataGeneratorService;

    @Autowired
    private PlayerUserLogRepository playerUserLogRepository;

    @BeforeEach
    void setUp() {
        // 각 테스트 전에 데이터 초기화
        playerUserLogRepository.deleteAllData();
        int testCount = 1_000_000;

        log.info("대규모 테스트 데이터 생성 시작: {} 건", testCount);

        long startTime = System.currentTimeMillis();
        testDataGeneratorService.generateJdbcTestData(testCount);
        long endTime = System.currentTimeMillis();

        long duration = endTime - startTime;
        long actualCount = testDataGeneratorService.getCurrentDataCount();

        log.info("대규모 테스트 데이터 생성 완료!");
        log.info("요청된 데이터 수: {} 건", testCount);
        log.info("실제 생성된 데이터 수: {} 건", actualCount);
        log.info("소요 시간: {} 초", duration / 1000.0);
    }

    @Test
    @DisplayName("PlayerUserLog 배치 테스트")
    void PlayerUserLog_배치_테스트(){
        //given
        long playerUserLogCount = playerUserLogRepository.countAllData();

        //when
        log.info("playUserLogCount: {}", playerUserLogCount);

        //then
    }
}
