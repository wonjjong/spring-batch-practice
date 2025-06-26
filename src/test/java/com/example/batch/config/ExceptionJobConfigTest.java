package com.example.batch.config;

import com.example.batch.TestConfig;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.springframework.batch.core.*;
import org.springframework.batch.core.explore.JobExplorer;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

@Slf4j
@SpringBootTest
@Import(TestConfig.class)
class ExceptionJobConfigTest {
    @Autowired
    private JobLauncher jobLauncher;

    @Autowired
    private Job exceptionJob;

    @Autowired
    private JobExplorer jobExplorer;

    @Test
    void runJobAndVerifyExecutionHistory() throws Exception {
        // given
        String jobName = exceptionJob.getName();
        JobParameters params = new JobParametersBuilder()
                .addLong("time", System.currentTimeMillis()) // JobInstance 구분용
                .toJobParameters();

        // when
        JobExecution jobExecution = jobLauncher.run(exceptionJob, params);

        // then
        assertThat(jobName).isEqualTo("exceptionJob");
        assertThat(jobExecution).isNotNull();
        assertThat(jobExecution.getJobInstance().getJobName()).isEqualTo(jobName);

        // 확인: JobExecution 상태 출력
        List<JobInstance> instances = jobExplorer.getJobInstances(jobName, 0, 10);
        JobInstance lastJobInstance = jobExplorer.getLastJobInstance(jobName);
        JobExecution lastJobExecution = jobExplorer.getLastJobExecution(lastJobInstance);
        log.info("▶ lastJobInstance = {}", lastJobInstance);
        log.info("▶ lastJobExecution = {}", lastJobExecution);
        log.info(">>>>>>> instances.size() = {}", instances.size());
        assertThat(instances).isNotEmpty();

        for (JobInstance instance : instances) {
            List<JobExecution> executions = jobExplorer.getJobExecutions(instance);
            for (JobExecution execution : executions) {
                log.info("▶ JobExecution ID: {}", execution.getId());
                log.info("   Status: {}", execution.getStatus());
                log.info("   Start: {}", execution.getStartTime());
                log.info("   End: {}", execution.getEndTime());
                log.info("   ExitStatus: {}", execution.getExitStatus());
                log.info("   step Count: {}", execution.getStepExecutions().size());
                log.info("-----------");

                for (StepExecution stepExecution : execution.getStepExecutions()) {
                    log.info(" \t▶ Step Name: {}", stepExecution.getStepName());
                    log.info("   \tStatus: {}", stepExecution.getStatus());
                    log.info("   \tRead Count: {}", stepExecution.getReadCount());
                    log.info("   \tWrite Count: {}", stepExecution.getWriteCount());
                    log.info("   \tCommit Count: {}", stepExecution.getCommitCount());
                    log.info("   \tExit Status: {}", stepExecution.getExitStatus());
                }
                // 테스트 확인: 실패한 실행이 포함되어야 함
                assertThat(execution.getStatus()).isIn(BatchStatus.COMPLETED, BatchStatus.FAILED);
            }
        }
    }

    @Test
    void runJobAndVerifyLastExecutionHistory() throws Exception {
        // given
        String jobName = exceptionJob.getName();
        JobParameters params = new JobParametersBuilder()
                .addLong("time", System.currentTimeMillis()) // JobInstance 구분용
                .toJobParameters();

        // when
        JobExecution jobExecution = jobLauncher.run(exceptionJob, params);

        // then
        assertThat(jobName).isEqualTo("exceptionJob");
        assertThat(jobExecution).isNotNull();
        assertThat(jobExecution.getJobInstance().getJobName()).isEqualTo(jobName);

        // 확인: JobExecution 상태 출력
        JobInstance lastJobInstance = jobExplorer.getLastJobInstance(jobName);
        JobExecution lastJobExecution = jobExplorer.getLastJobExecution(lastJobInstance);
        log.info("▶ lastJobInstance = {}", lastJobInstance);
        log.info("▶ lastJobExecution = {}", lastJobExecution);

        List<JobExecution> executions = jobExplorer.getJobExecutions(lastJobInstance);
        for (JobExecution execution : executions) {
            log.info("▶ JobExecution ID: {}", execution.getId());
            log.info("   Status: {}", execution.getStatus());
            log.info("   Start: {}", execution.getStartTime());
            log.info("   End: {}", execution.getEndTime());
            log.info("   ExitStatus: {}", execution.getExitStatus());
            log.info("   step Count: {}", execution.getStepExecutions().size());
            log.info("-----------");

            for (StepExecution stepExecution : execution.getStepExecutions()) {
                log.info(" \t▶ Step Name: {}", stepExecution.getStepName());
                log.info("   \tStatus: {}", stepExecution.getStatus());
                log.info("   \tRead Count: {}", stepExecution.getReadCount());
                log.info("   \tWrite Count: {}", stepExecution.getWriteCount());
                log.info("   \tCommit Count: {}", stepExecution.getCommitCount());
                log.info("   \tExit Status: {}", stepExecution.getExitStatus());
            }
            // 테스트 확인: 실패한 실행이 포함되어야 함
            assertThat(execution.getStatus()).isIn(BatchStatus.COMPLETED, BatchStatus.FAILED);
        }
    }
}