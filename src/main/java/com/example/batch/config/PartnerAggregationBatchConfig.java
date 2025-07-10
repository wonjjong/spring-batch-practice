package com.example.batch.config;

import com.example.batch.entity.PartnerAggregation;
import com.example.batch.service.PartnerAggregationService;
import jakarta.persistence.EntityManager;
import jakarta.persistence.EntityManagerFactory;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobScope;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.item.*;
import org.springframework.batch.item.database.JpaItemWriter;
import org.springframework.batch.item.support.ListItemReader;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.transaction.PlatformTransactionManager;

import java.time.LocalDateTime;
import java.util.List;

@Configuration
@RequiredArgsConstructor
@Slf4j
public class PartnerAggregationBatchConfig {

    private final JobRepository jobRepository;
    private final PlatformTransactionManager transactionManager;
    private final PartnerAggregationService partnerAggregationService;
    private final EntityManagerFactory entityManagerFactory;
    private static Long count = 0L;

    @Bean
    public Job partnerAggregationJob(Step partnerAggregationStep) {
        return new JobBuilder("partnerAggregationJob", jobRepository)
                .start(partnerAggregationStep)
                .build();
    }

    @Bean
    public Step partnerAggregationStep(
//            ItemReader<PartnerAggregation> reader,
//           ItemProcessor<PartnerAggregation, PartnerAggregation> processor,
//           ItemWriter<PartnerAggregation> writer
            Tasklet partnerAggregationTasklet
    ) {
        return new StepBuilder("partnerAggregationStep", jobRepository)
                .tasklet(partnerAggregationTasklet, transactionManager)
                .build();
//        return new StepBuilder("partnerAggregationStep", jobRepository)
//                .<PartnerAggregation, PartnerAggregation>chunk(1, transactionManager)
//                .reader(reader)
//                .processor(processor)
//                .writer(writer)
//                .build();
    }

    @Bean
    @StepScope
    public ItemReader<PartnerAggregation> partnerAggregationListItemReader(
            @Value("#{jobParameters['startDateTime']}") LocalDateTime startDateTime,
            @Value("#{jobParameters['endDateTime']}") LocalDateTime endDateTime) {
        log.info("Starting Partner Aggregation List Item Reader with startDateTime: {}, endDateTime: {}",
                startDateTime, endDateTime);

        List<PartnerAggregation> aggregateList = partnerAggregationService.aggregateByDateRange(startDateTime, endDateTime);
        log.info("Found {} Partner Aggregations", aggregateList.size());

        return new LoggingItemReader<>(new ListItemReader<>(aggregateList));
    }

    @Bean
    @StepScope
    public ItemProcessor<PartnerAggregation, PartnerAggregation> partnerAggregationItemProcessor() {
        return partnerAggregation -> {
            if(count == 2) throw new RuntimeException("Retry Test: 2번째 아이템에서 강제 예외 발생");
            log.info("Processing item: {}", partnerAggregation);
            return partnerAggregation;
        };
    }

    @Bean
    @StepScope
    public JpaItemWriter<PartnerAggregation> partnerAggregationItemWriter(
        @Value("#{jobParameters['isRetryTest']}") Long isRetryTest
    ) {
        JpaItemWriter<PartnerAggregation> partnerAggregationJpaItemWriter = new JpaItemWriter<>();
        partnerAggregationJpaItemWriter.setEntityManagerFactory(entityManagerFactory);
//
//        return new JpaItemWriter<>() {
//            public void write(Chunk<? extends PartnerAggregation> chunk) {
//                count++;
//                if (isRetryTest == 1L && count == 2) {
//                    throw new RuntimeException("Retry Test: 2번째 청크에서 강제 예외 발생");
//                }
//
//                List<PartnerAggregation> items = (List<PartnerAggregation>) chunk.getItems();
//                log.info("Writing chunk {} with {} items", count, items);
//                partnerAggregationService.saveBatchData(items);
//            }
//        };
        return partnerAggregationJpaItemWriter;
    }

    @Bean
    @StepScope
    public Tasklet partnerAggregationTasklet(
            @Value("#{jobParameters['startDateTime']}") LocalDateTime startDateTime,
            @Value("#{jobParameters['endDateTime']}") LocalDateTime endDateTime) {
        return (contribution, chunkContext) -> {
            count++;
            if(count ==1) throw new RuntimeException("Retry Test: 1번째 아이템에서 강제 예외 발생");
            List<PartnerAggregation> aggregateList = partnerAggregationService.aggregateByDateRange(startDateTime, endDateTime);

            log.info("Partner ID별 집계 배치 작업 시작");
            partnerAggregationService.saveBatchData(aggregateList);
            return RepeatStatus.FINISHED;
        };
    }

    public class LoggingItemReader<T> implements ItemReader<T> {
        private final ItemReader<T> delegate;

        public LoggingItemReader(ItemReader<T> delegate) {
            this.delegate = delegate;
        }

        @Override
        public T read() throws UnexpectedInputException, ParseException, NonTransientResourceException, Exception {
            T item = delegate.read();
            log.info("Read item: {}", item);
            return item;
        }
    }
} 