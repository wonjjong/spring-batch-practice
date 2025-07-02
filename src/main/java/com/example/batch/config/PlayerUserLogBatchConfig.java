package com.example.batch.config;

import com.example.batch.entity.PlayerUserLog;
import com.example.batch.reader.PlayerUserLogItemReader;
import com.example.batch.reader.PlayerUserLogJdbcItemReader;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.database.JdbcCursorItemReader;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.transaction.PlatformTransactionManager;

@Configuration
@RequiredArgsConstructor
@Slf4j
public class PlayerUserLogBatchConfig {

    private final JobRepository jobRepository;
    private final PlatformTransactionManager transactionManager;
    private final PlayerUserLogJdbcItemReader playerUserLogJdbcItemReader;
    private final PlayerUserLogItemReader playerUserLogItemReader;

    @Bean
    public Job playerUserLogJob() {
        return new JobBuilder("playerUserLogJob", jobRepository)
                .start(playerUserLogStep())
                .build();
    }

    @Bean
    public Step playerUserLogStep() {
        return new StepBuilder("playerUserLogStep", jobRepository)
                .<PlayerUserLog, PlayerUserLog>chunk(1000, transactionManager)
                .reader(playerUserLogJdbcItemReader.createReader())
                .processor(playerUserLogProcessor())
                .writer(playerUserLogWriter())
                .build();
    }

    @Bean
    public ItemProcessor<PlayerUserLog, PlayerUserLog> playerUserLogProcessor() {
        return item -> {
            // 여기서 데이터 처리 로직을 구현
            // 예: 데이터 검증, 변환, 집계 등
            log.debug("처리 중: {}", item.getBroadcastId());
            return item;
        };
    }

    @Bean
    public ItemWriter<PlayerUserLog> playerUserLogWriter() {
        return items -> {
            // 여기서 처리된 데이터를 출력하거나 다른 저장소에 저장
            log.info("처리된 데이터 수: {} 건", items.size());
            
            // 실제 구현에서는 다음과 같은 작업을 수행할 수 있습니다:
            // 1. 집계 데이터를 별도 테이블에 저장
            // 2. 파일로 출력
            // 3. 외부 API 호출
            // 4. 알림 발송 등
            
            for (PlayerUserLog item : items) {
                // 예시: 각 액션별 카운트 집계
                switch (item.getAction()) {
                    case "pageView":
                        log.debug("PageView 처리: {}", item.getMemberId());
                        break;
                    case "chat":
                        log.debug("Chat 처리: {}", item.getMemberId());
                        break;
                    case "productOrder":
                        log.debug("ProductOrder 처리: {} - 금액: {}", item.getMemberId(), item.getProductOrderAmount());
                        break;
                    default:
                        log.debug("기타 액션 처리: {} - {}", item.getAction(), item.getMemberId());
                }
            }
        };
    }
} 