package com.example.batch;

import com.example.batch.entity.PlayerUserLog;
import com.example.batch.repository.PlayerUserLogRepository;
import com.example.batch.service.TestDataGeneratorService;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.test.JobLauncherTestUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.ActiveProfiles;

import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest
@Import(TestConfig.class)
@ActiveProfiles("test")
@Slf4j
public class PlayerUserlogBatchTest {
    
    @Autowired
    private JobLauncher jobLauncher;

    @Autowired
    private Job playerUserLogJob;

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
    void PlayerUserLog_배치_테스트() throws Exception {
        // given
        long playerUserLogCount = playerUserLogRepository.countAllData();
        log.info("처리할 데이터 수: {} 건", playerUserLogCount);

        // when
        JobParameters jobParameters = new JobParametersBuilder()
                .addLong("time", System.currentTimeMillis())
                .toJobParameters();

        JobExecution jobExecution = jobLauncherTestUtils.launchJob(jobParameters);

        // then
        assertEquals(BatchStatus.COMPLETED, jobExecution.getStatus());
        log.info("배치 작업 완료: {}", jobExecution.getStatus());
        log.info("처리된 청크 수: {}", jobExecution.getStepExecutions().iterator().next().getCommitCount());
    }

    @Test
    @DisplayName("PlayerUserLog 배치 성능 테스트")
    void PlayerUserLog_배치_성능_테스트() throws Exception {
        // given
        long playerUserLogCount = playerUserLogRepository.countAllData();
        log.info("성능 테스트 - 처리할 데이터 수: {} 건", playerUserLogCount);

        // when
        long startTime = System.currentTimeMillis();
        
        JobParameters jobParameters = new JobParametersBuilder()
                .addLong("time", System.currentTimeMillis())
                .toJobParameters();

        JobExecution jobExecution = jobLauncherTestUtils.launchJob(jobParameters);
        
        long endTime = System.currentTimeMillis();
        long duration = endTime - startTime;

        // then
        assertEquals(BatchStatus.COMPLETED, jobExecution.getStatus());
        log.info("배치 성능 테스트 완료!");
        log.info("총 소요 시간: {} 초", duration / 1000.0);
        log.info("초당 처리 건수: {} 건/초", playerUserLogCount / (duration / 1000.0));
        
        // 성능 검증 (100만건 기준 5분 이내 완료)
        assertTrue(duration < 300000, "100만건 처리는 5분 이내에 완료되어야 합니다.");
    }

    @Test
    @DisplayName("PlayerUserLog 데이터 검증 테스트")
    void PlayerUserLog_데이터_검증_테스트() {
        // given
        long totalCount = playerUserLogRepository.countAllData();
        
        // when & then
        assertTrue(totalCount > 0, "데이터가 존재해야 합니다.");
        log.info("총 데이터 수: {} 건", totalCount);
        
        // 샘플 데이터 검증
        PlayerUserLog sampleData = playerUserLogRepository.findAll().stream().findFirst().orElse(null);
        assertNotNull(sampleData, "샘플 데이터가 존재해야 합니다.");
        
        log.info("샘플 데이터: {}", sampleData);
        assertNotNull(sampleData.getBroadcastId(), "broadcast_id는 null이 아니어야 합니다.");
        assertNotNull(sampleData.getMemberId(), "member_id는 null이 아니어야 합니다.");
        assertNotNull(sampleData.getAction(), "action은 null이 아니어야 합니다.");
        assertEquals("live", sampleData.getBroadcastTypeCode(), "broadcast_type_code는 'live'여야 합니다.");
    }
}
