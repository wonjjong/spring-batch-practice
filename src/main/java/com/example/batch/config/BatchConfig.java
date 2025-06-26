package com.example.batch.config;

import com.example.batch.dto.UserDto;
import com.example.batch.entity.UserEntity;
import com.example.batch.repository.UserRepository;
import jakarta.persistence.EntityManagerFactory;
import lombok.RequiredArgsConstructor;
import org.springframework.batch.core.*;
import org.springframework.batch.core.configuration.annotation.*;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.*;
import org.springframework.batch.item.database.*;
import org.springframework.batch.item.database.builder.JpaItemWriterBuilder;
import org.springframework.batch.item.database.builder.JdbcPagingItemReaderBuilder;
import org.springframework.batch.item.support.ListItemReader;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.*;
import org.springframework.core.io.ClassPathResource;
import org.springframework.jdbc.datasource.init.DataSourceInitializer;
import org.springframework.jdbc.datasource.init.ResourceDatabasePopulator;
import org.springframework.transaction.PlatformTransactionManager;

import javax.sql.DataSource;
import java.util.*;

@Configuration
@EnableBatchProcessing
@RequiredArgsConstructor
public class BatchConfig {
    private final JobRepository jobRepository;
    private final PlatformTransactionManager transactionManager;
    private final EntityManagerFactory entityManagerFactory;

    @Bean
    public Job userJob(Step userStep) {
        return new JobBuilder("userJob", jobRepository)
                .incrementer(new RunIdIncrementer())
                .start(userStep)
                .build();
    }

    @Bean
    public Step userStep(ItemReader<UserDto> userReader,
                         ItemProcessor<UserDto, UserEntity> userProcessor,
                         ItemWriter<UserEntity> userWriter) {

        return new StepBuilder("userStep", jobRepository)
                .<UserDto, UserEntity>chunk(10, transactionManager)
                .reader(userReader)
                .processor(userProcessor)
                .writer(userWriter)
                .build();
    }

    @Bean
    public ItemReader<UserDto> userReader() {
        List<UserDto> mockData = new ArrayList<>();
        for (int i = 1; i <= 100; i++) {
            mockData.add(new UserDto("User" + i, "user" + i + "@example.com"));
        }
        return new ListItemReader<>(mockData);
    }

    @Bean
    public ItemProcessor<UserDto, UserEntity> userProcessor() {
        return dto -> new UserEntity(null, dto.getName(), dto.getEmail());
    }

    @Bean
    public JpaItemWriter<UserEntity> userWriter() {
        return new JpaItemWriterBuilder<UserEntity>()
                .entityManagerFactory(entityManagerFactory)
                .build();
    }

}
