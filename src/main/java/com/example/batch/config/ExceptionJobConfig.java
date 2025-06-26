package com.example.batch.config;


import com.example.batch.dto.UserDto;
import com.example.batch.entity.UserEntity;
import jakarta.persistence.EntityManagerFactory;
import lombok.RequiredArgsConstructor;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.database.JpaItemWriter;
import org.springframework.batch.item.database.builder.JpaItemWriterBuilder;
import org.springframework.batch.item.support.ListItemReader;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.transaction.PlatformTransactionManager;

import java.util.ArrayList;
import java.util.List;

@RequiredArgsConstructor
@Configuration
public class ExceptionJobConfig {
    private final JobRepository jobRepository;
    private final PlatformTransactionManager transactionManager;
    private final EntityManagerFactory entityManagerFactory;

    @Bean
    public Job exceptionJob(Step exceptionStep) {
        return new JobBuilder("exceptionJob", jobRepository)
                .start(exceptionStep)
                .build();
    }

    @Bean
    public Step exceptionStep(ItemReader<UserDto> userExceptionReader,
                              ItemProcessor<UserDto, UserEntity> userExceptionProcessor,
                              ItemWriter<UserEntity> userExceptionWriter) {
        return new StepBuilder("exceptionStep", jobRepository)
                .<UserDto, UserEntity>chunk(10, transactionManager)
                .reader(userExceptionReader)
                .processor(userExceptionProcessor)
                .writer(userExceptionWriter)
                .build();
    }

    @Bean
    public ItemReader<UserDto> userExceptionReader() {
        List<UserDto> mockData = new ArrayList<>();
        for (int i = 1; i <= 100; i++) {
            mockData.add(new UserDto("User" + i, "user" + i + "@example.com"));
        }
        return new ListItemReader<>(mockData);
    }

    @Bean
    public ItemProcessor<UserDto, UserEntity> userExceptionProcessor() {
        return new ItemProcessor<UserDto, UserEntity>() {
            private int count = 0;

            @Override
            public UserEntity process(UserDto dto) throws Exception {
                count++;
                System.out.println("processor count = " + count);
                if(count > 50 && count <= 60) {
                    throw new RuntimeException("Simulated exception for testing");
                }
                return new UserEntity(null, dto.getName(), dto.getEmail());
            }
        };
    }

    @Bean
    public JpaItemWriter<UserEntity> userExceptionWriter() {
        return new JpaItemWriterBuilder<UserEntity>()
                .entityManagerFactory(entityManagerFactory)
                .build();
    }
}
