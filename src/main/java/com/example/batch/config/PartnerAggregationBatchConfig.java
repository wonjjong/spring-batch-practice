package com.example.batch.config;

import com.example.batch.entity.PartnerAggregation;
import com.example.batch.service.PartnerAggregationService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.transaction.PlatformTransactionManager;

import java.time.LocalDateTime;

@Configuration
@RequiredArgsConstructor
@Slf4j
public class PartnerAggregationBatchConfig {

    private final JobRepository jobRepository;
    private final PlatformTransactionManager transactionManager;
    private final PartnerAggregationService partnerAggregationService;

    @Bean
    public Job partnerAggregationJob() {
        return new JobBuilder("partnerAggregationJob", jobRepository)
                .start(partnerAggregationStep())
                .build();
    }

    @Bean
    public Step partnerAggregationStep() {
        return new StepBuilder("partnerAggregationStep", jobRepository)
                .tasklet(partnerAggregationTasklet(), transactionManager)
                .build();
    }

    @Bean
    public Tasklet partnerAggregationTasklet() {
        return (contribution, chunkContext) -> {
            log.info("Partner ID별 집계 배치 작업 시작");
            
            // 집계 날짜 설정 (현재 시간 기준)
            LocalDateTime aggregationDate = LocalDateTime.now();
            
            // Partner ID별 집계 수행
            partnerAggregationService.aggregateByPartnerId(aggregationDate);
            
            // 집계 결과 조회 및 로깅
            var aggregations = partnerAggregationService.getAggregationsByDate(aggregationDate);
            log.info("집계 완료: {} 개 파트너", aggregations.size());
            
            for (PartnerAggregation agg : aggregations) {
                log.info("파트너 {}: UV={}, PV={}, 채팅={}, 주문={}, 주문금액={}", 
                    agg.getPartnerId(), 
                    agg.getTotalUv(), 
                    agg.getTotalPv(), 
                    agg.getTotalChatCount(), 
                    agg.getTotalProductOrderCount(), 
                    agg.getTotalProductOrderAmount());
            }
            
            return RepeatStatus.FINISHED;
        };
    }
} 