package com.example.batch;


import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import javax.sql.DataSource;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.support.ListItemReader;
import org.springframework.batch.test.JobLauncherTestUtils;
import org.springframework.batch.test.context.SpringBatchTest;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.PlatformTransactionManager;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@SpringBootTest
@SpringBatchTest
@Import({TestConfig.class, ChunkRetryTest.BatchConfig.class,})
public class ChunkRetryTest {
    @Autowired
    private JobLauncherTestUtils jobLauncherTestUtils;
    @Autowired
    private DataSource dataSource;

    @BeforeEach
    void setup() {
        JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
        try {
            jdbcTemplate.execute("CREATE TABLE test_table (id INT PRIMARY KEY, \"value\" VARCHAR(100))");
        } catch (Exception ignored) {
            // 이미 테이블이 있으면 무시
        }
        jdbcTemplate.execute("DELETE FROM test_table");
        failedData.clear();
        chunkCount.set(0);
    }


    // 실패 데이터 임시 저장소 (실무에서는 별도 테이블 사용)
    private static final List<Integer> failedData = new ArrayList<>();
    private static final AtomicInteger chunkCount = new AtomicInteger(0);

    @Test
    void testResumeAndIdempotent() throws Exception {
        // 1차 실행: 2번째 Chunk에서 실패
        Assertions.assertThrows(Exception.class, () -> jobLauncherTestUtils.launchJob());
        // 첫 번째 Chunk만 성공
        JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
        int countAfterFail = jdbcTemplate.queryForObject("SELECT COUNT(*) FROM test_table", Integer.class);
        log.info("count after fail: {}", countAfterFail);
        Assertions.assertEquals(3, countAfterFail);
        // 실패 데이터가 별도 저장소에 기록됐는지 확인
        Assertions.assertFalse(failedData.isEmpty());
        // 2차 실행: Resume
        JobExecution restartExecution = jobLauncherTestUtils.launchJob();
        Assertions.assertEquals(BatchStatus.COMPLETED, restartExecution.getStatus());
        int countAfterResume = jdbcTemplate.queryForObject("SELECT COUNT(*) FROM test_table", Integer.class);
        Assertions.assertEquals(10, countAfterResume);
    }

    @TestConfiguration
    static class BatchConfig {
        @Autowired
        private JobRepository jobRepository;
        @Autowired
        private PlatformTransactionManager transactionManager;
        @Autowired
        private DataSource dataSource;

        @Bean
        public Job testJob() {
            return new JobBuilder("testJob", jobRepository)
                    .start(testStep())
                    .build();
        }

        @Bean
        public Step testStep() {
            return new StepBuilder("testStep", jobRepository)
                    .<Integer, Integer>chunk(3, transactionManager)
                    .reader(new ListItemReader<>(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)))
                    .processor(item -> item)
                    .writer(items -> {
                        // 2번째 Chunk에서 일부러 예외 발생
                        if (chunkCount.incrementAndGet() == 2) {
                            // 실패 데이터 별도 저장
                            failedData.addAll((List<Integer>) items);
                            throw new RuntimeException("Fail at 2nd chunk");
                        }
                        // 멱등성 보장: 중복 insert 방지
                        JdbcTemplate jdbc = new JdbcTemplate(dataSource);
                        for (Integer item : items) {
                            jdbc.update("INSERT IGNORE INTO test_table (id, \"value\") VALUES (?, ?)", item, "val" + item);
//                            jdbc.update("INSERT IGNORE INTO test_table (id, value) VALUES (?, ?)", item, "val" + item);
                        }
                    })
                    .build();
        }
    }
}
