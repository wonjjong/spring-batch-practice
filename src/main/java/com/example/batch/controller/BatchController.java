package com.example.batch.controller;


import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.time.LocalDateTime;

@Slf4j
@RequiredArgsConstructor
@RestController
public class BatchController {
    private final Job partnerAggregationJob;
    private final JobLauncher jobLauncher;

    @GetMapping("/batch")
    public void batch(LocalDateTime startDateTime, LocalDateTime endDateTime, String batchType, String partnerId) {
        JobParameters jobParameters = new JobParametersBuilder()
                .addString("batchType", batchType)
                .addLocalDateTime("startDateTime", startDateTime)
                .addLocalDateTime("endDateTime", endDateTime)
                .addString("partnerId", partnerId) // partner_001
                .toJobParameters();


        log.info("Batch job started with parameters: {}", jobParameters);

        try {
            jobLauncher.run(partnerAggregationJob, jobParameters);
        } catch (Exception e) {
            log.error("Batch job failed with parameters: {}", jobParameters, e);
        }
    }

}
