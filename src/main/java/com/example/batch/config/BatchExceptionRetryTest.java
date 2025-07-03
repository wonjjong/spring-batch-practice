package com.example.batch.config;

import jakarta.persistence.EntityManagerFactory;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.transaction.PlatformTransactionManager;

import java.util.List;

@Slf4j
@Configuration
@EnableBatchProcessing
@RequiredArgsConstructor
public class BatchExceptionRetryTest {
    private final JobRepository jobRepository;
    private final PlatformTransactionManager transactionManager;
    private final EntityManagerFactory entityManagerFactory;
    private final List<Integer> mockList = List.of(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);

    @Bean
    public Job exceptionRetryJob() {
        return new JobBuilder("exceptionRetryJob", jobRepository)
                .start(retryStep())
                .build();
    }

    @Bean
    public Step retryStep() {
        return new StepBuilder("retryStep", jobRepository)
                .<Integer, Integer>chunk(4, transactionManager)
                .reader(itemReader())
                .processor(itemProcessor())
                .writer(itemWriter())
                .faultTolerant()
                .retry(RuntimeException.class) // RuntimeException 발생 시 재시도
                .retryLimit(3) // 최대 3회 재시도
                .build();
    }

    @Bean
    public ItemReader<Integer> itemReader() {
        return new ItemReader<>() {
            private int index = 0;

            @Override
            public Integer read() {
                if (index < mockList.size()) {
                    return mockList.get(index++);
                }
                return null;
            }
        };
    }

    @Bean
    public ItemProcessor<Integer, Integer> itemProcessor() {
        return new ItemProcessor<>() {
            private int retryCount = 0;

            @Override
            public Integer process(Integer item) {
                if (item == 5) {
                    retryCount++;
                    log.info("Processing failed for item: {}, retryCount: {}", item, retryCount);
                    throw new RuntimeException("Retry from processor for item: " + item);
                }
                log.info("Processing item: {}", item);
                return item;
            }
        };
    }

    @Bean
    public ItemWriter<Integer> itemWriter() {
        return items -> {
            for (Integer item : items) {
                log.info("Writing item: {}", item);
            }
        };
    }

    //RetryListener

}
