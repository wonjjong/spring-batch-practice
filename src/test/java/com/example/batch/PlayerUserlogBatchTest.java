package com.example.batch;

import com.example.batch.entity.PlayerUserLog;
import com.example.batch.repository.PlayerUserLogRepository;
import com.example.batch.service.TestDataGeneratorService;
import jakarta.transaction.Transactional;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.test.JobLauncherTestUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;

@SpringBootTest
@Import(TestConfig.class)
@Slf4j
public class PlayerUserlogBatchTest {
    @Autowired
    private JobLauncher jobLauncher;

    @Autowired
    private Job userJob;

    @Autowired
    private JobLauncherTestUtils jobLauncherTestUtils;

    @Autowired
    private JobRepository jobRepository;

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

    @Bean
    public ItemReader<PlayerUserLog> playerUserLogReader(){
        return new ItemReader<PlayerUserLog>() {
            @Override
            public PlayerUserLog read() {
                // 실제 구현은 데이터베이스에서 PlayerUserLog를 읽어오는 로직이 필요합니다.
                // 여기서는 단순히 null을 반환하여 더 이상 읽을 데이터가 없음을 나타냅니다.
                return null;
            }
        };
    }

}
