package com.example.batch;

import org.junit.jupiter.api.Test;
import org.springframework.batch.core.*;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;


import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest
class BatchTest {
    @Autowired
    private JobLauncher jobLauncher;

    @Autowired
    private Job userJob;

    @Test
    void job_실행_테스트() throws Exception {
        JobParameters params = new JobParametersBuilder()
                .addLong("timestamp", System.currentTimeMillis())
                .toJobParameters();

        JobExecution execution = jobLauncher.run(userJob, params);

        assertThat(execution.getStatus()).isEqualTo(BatchStatus.COMPLETED);
    }
}