package com.example.batch;


import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import javax.sql.DataSource;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.batch.core.*;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
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

import static org.assertj.core.api.Assertions.assertThat;

@Slf4j
@SpringBootTest
@SpringBatchTest
@Import({TestConfig.class, ChunkRetryTest.BatchConfig.class,})
public class ChunkRetryTest {
    @Autowired
    private JobLauncherTestUtils jobLauncherTestUtils;
    @Autowired
    private DataSource dataSource;
    @Autowired
    private Job testJob;

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
        JobParameters firstJobParameter = new JobParametersBuilder()
                .addLong("run.id", System.currentTimeMillis())
                .toJobParameters();
        jobLauncherTestUtils.setJob(testJob);

        JobExecution jobExecution = jobLauncherTestUtils.launchJob(firstJobParameter);
        log.info(">>>>>>>>>>>>>>>>>>>. jobExecution: {}", jobExecution);
        // 1차 실행: 2번째 Chunk에서 실패
        assertThat(jobExecution.getStatus()).isEqualTo(BatchStatus.FAILED);
        // 첫 번째 Chunk만 성공
        JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
        int countAfterFail = jdbcTemplate.queryForObject("SELECT COUNT(*) FROM test_table", Integer.class);
        log.info("count after fail: {}", countAfterFail);
        Assertions.assertEquals(3, countAfterFail);
        // 실패 데이터가 별도 저장소에 기록됐는지 확인
        Assertions.assertFalse(failedData.isEmpty());
        // 2차 실행: Resume
        log.info("-----------------------------------------------------------");

        JobExecution restartExecution = jobLauncherTestUtils.launchJob(firstJobParameter);
        log.info("<<<<<<<<<<<< restartExecution: {}", restartExecution);
        Assertions.assertEquals(BatchStatus.COMPLETED, restartExecution.getStatus());
        List<String> allRows = jdbcTemplate.query(
                "SELECT id, \"value\" FROM test_table ORDER BY id",
                (rs, rowNum) -> "id=" + rs.getInt("id") + ", value=" + rs.getString("value")
        );
        allRows.forEach(row -> log.info("ROW: {}", row));
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
                    .incrementer(new RunIdIncrementer())
                    .start(testStep())
                    .build();
        }

        @Bean
        @StepScope
        public ListItemReader<Integer> listItemReader() {
            return new ListItemReader<>(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10));
        }

        @Bean
        public Step testStep() {
            return new StepBuilder("testStep", jobRepository)
                    .<Integer, Integer>chunk(3, transactionManager)
                    .reader(listItemReader())
                    .processor(item -> item)
                    .writer(items -> {
                        List<Integer> chunkItems = new ArrayList<>(items.getItems()); // ★ 타입 캐스팅 대신 복사

                        // 2번째 Chunk에서 일부러 예외 발생
                        if (chunkItems.contains(4) && failedData.isEmpty()) {
                            // 실패 데이터 별도 저장
                            failedData.addAll(items.getItems());
                            throw new RuntimeException("Fail at 2nd chunk");
                        }
                        log.info(chunkCount.get() + " chunk: " + items.getItems());
                        // 멱등성 보장: 중복 insert 방지
                        JdbcTemplate jdbc = new JdbcTemplate(dataSource);
                        for (Integer item : items) {
                            jdbc.update("MERGE INTO test_table (id, \"value\") VALUES (?, ?)", item, "val" + item);
                        }
                    })
                    .build();
        }
    }
}
