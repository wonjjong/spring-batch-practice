package com.example.batch;


import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.repository.support.JobRepositoryFactoryBean;
import org.springframework.batch.test.JobLauncherTestUtils;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.core.io.ClassPathResource;
import org.springframework.jdbc.datasource.init.DataSourceInitializer;
import org.springframework.jdbc.datasource.init.ResourceDatabasePopulator;
import org.springframework.orm.jpa.JpaTransactionManager;
import org.springframework.orm.jpa.LocalContainerEntityManagerFactoryBean;
import org.springframework.orm.jpa.vendor.HibernateJpaVendorAdapter;
import org.springframework.transaction.PlatformTransactionManager;

import javax.sql.DataSource;
import java.util.Properties;

@Configuration
public class TestConfig {

    @Bean
    public JobLauncherTestUtils jobLauncherTestUtils() {
        return new JobLauncherTestUtils();
    }

    // Meta DataSource 설정 (Spring Batch 메타데이터용)
    @Bean
    @ConfigurationProperties("spring.datasource.meta.hikari")
    public HikariConfig metaHikariConfig() {
        return new HikariConfig();
    }

    @Bean
    @Primary  // Spring Batch가 기본적으로 사용할 DataSource
    public DataSource dataSource(HikariConfig metaHikariConfig) {
        return new HikariDataSource(metaHikariConfig);
    }
    
    // Domain DataSource 설정 (엔티티 저장용)
    @Bean
    @ConfigurationProperties("spring.datasource.domain.hikari")
    public HikariConfig domainHikariConfig() {
        return new HikariConfig();
    }
    
    @Bean
    @Qualifier("domainDataSource")
    public DataSource domainDataSource(HikariConfig domainHikariConfig) {
        return new HikariDataSource(domainHikariConfig);
    }
    
    // JPA EntityManagerFactory 설정 (domainDataSource 사용)
    @Bean
    public LocalContainerEntityManagerFactoryBean entityManagerFactory(@Qualifier("domainDataSource") DataSource domainDataSource) {
        LocalContainerEntityManagerFactoryBean em = new LocalContainerEntityManagerFactoryBean();
        em.setDataSource(domainDataSource);
        em.setPackagesToScan("com.example.batch.entity");
        
        HibernateJpaVendorAdapter vendorAdapter = new HibernateJpaVendorAdapter();
        em.setJpaVendorAdapter(vendorAdapter);
        
        Properties properties = new Properties();
        properties.setProperty("hibernate.hbm2ddl.auto", "create-drop");
        properties.setProperty("hibernate.format_sql", "false");
        properties.setProperty("hibernate.show_sql", "false");
        em.setJpaProperties(properties);
        
        return em;
    }
    
    // JPA 트랜잭션 매니저 설정
    @Bean
    public PlatformTransactionManager transactionManager(LocalContainerEntityManagerFactoryBean entityManagerFactory) {
        JpaTransactionManager transactionManager = new JpaTransactionManager();
        transactionManager.setEntityManagerFactory(entityManagerFactory.getObject());
        return transactionManager;
    }

    @Bean
    public DataSourceInitializer metaDataSourceInitializer(DataSource dataSource) {
        DataSourceInitializer dataSourceInitializer = new DataSourceInitializer();
        dataSourceInitializer.setDataSource(dataSource);
        ResourceDatabasePopulator databasePopulator = new ResourceDatabasePopulator();
        databasePopulator.addScript(new ClassPathResource("org/springframework/batch/core/schema-h2.sql"));
        dataSourceInitializer.setDatabasePopulator(databasePopulator);
        return dataSourceInitializer;
    }
    
    // Spring Batch 5.0+ 방식으로 JobRepository 설정
    @Bean
    public JobRepository jobRepository(DataSource dataSource, PlatformTransactionManager transactionManager) throws Exception {
        JobRepositoryFactoryBean factory = new JobRepositoryFactoryBean();
        factory.setDataSource(dataSource);
        factory.setTransactionManager(transactionManager);
        factory.afterPropertiesSet();
        return factory.getObject();
    }
}
