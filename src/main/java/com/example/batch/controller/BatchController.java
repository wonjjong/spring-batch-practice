package com.example.batch.controller;


import com.example.batch.service.PartnerAggregationService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.time.LocalDateTime;

@Slf4j
@RequiredArgsConstructor
@RestController
public class BatchController {
    private final PartnerAggregationService partnerAggregationService;


    @GetMapping("/batch")
    public void batch(LocalDateTime startDateTime, LocalDateTime endDateTime, String batchType, String partnerId) {
        JobParameters jobParameters = new JobParametersBuilder()
                .addString("batchType", batchType)
                .addDate("startDateTime", java.util.Date.from(startDateTime.atZone(java.time.ZoneId.systemDefault()).toInstant()))
                .addDate("endDateTime", java.util.Date.from(endDateTime.atZone(java.time.ZoneId.systemDefault()).toInstant()))
                .addString("partnerId",partnerId) // partner_001
                .toJobParameters();

        log.info("Batch job started with parameters: {}", jobParameters);
    }

}
