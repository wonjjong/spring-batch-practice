package com.example.batch;

import com.example.batch.entity.PartnerAggregation;
import com.example.batch.service.PartnerAggregationService;
import com.example.batch.service.TestDataGeneratorService;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.test.JobLauncherTestUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.ActiveProfiles;

import java.time.LocalDateTime;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest
@Import(TestConfig.class)
@ActiveProfiles("test")
@Slf4j
class PartnerAggregationTest {

    @Autowired
    private JobLauncherTestUtils jobLauncherTestUtils;

    @Autowired
    private TestDataGeneratorService testDataGeneratorService;

    @Autowired
    private PartnerAggregationService partnerAggregationService;

    @BeforeEach
    void setUp() {
        // 테스트 데이터 생성 (1만건)
        int testCount = 100_00;
        log.info("테스트 데이터 생성 시작: {} 건", testCount);

        long startTime = System.currentTimeMillis();
        testDataGeneratorService.generateJdbcTestData(testCount);
        long endTime = System.currentTimeMillis();

        log.info("테스트 데이터 생성 완료: {} 초", (endTime - startTime) / 1000.0);
    }

    @Test
    @DisplayName("Partner ID별 집계 서비스 테스트")
    void partnerAggregation_서비스_테스트() {
        // given
        LocalDateTime aggregationDate = LocalDateTime.now();

        // when
        long startTime = System.currentTimeMillis();
        partnerAggregationService.aggregateByPartnerId(aggregationDate);
        long endTime = System.currentTimeMillis();

        // then
        List<PartnerAggregation> aggregations = partnerAggregationService.getAggregationsByDate(aggregationDate);

        log.info("집계 완료: {} 개 파트너, 소요시간: {} 초",
                aggregations.size(), (endTime - startTime) / 1000.0);

        assertFalse(aggregations.isEmpty(), "집계 결과가 존재해야 합니다.");

        // 각 파트너별 집계 결과 검증
        for (PartnerAggregation agg : aggregations) {
            assertNotNull(agg.getPartnerId(), "partner_id는 null이 아니어야 합니다.");
            assertTrue(agg.getTotalUv() >= 0, "UV는 0 이상이어야 합니다.");
            assertTrue(agg.getTotalPv() >= 0, "PV는 0 이상이어야 합니다.");

            log.info("파트너 {} 집계 결과:", agg.getPartnerId());
            log.info("  - UV: {}", agg.getTotalUv());
            log.info("  - PV: {}", agg.getTotalPv());
            log.info("  - 총 재생시간: {}", agg.getTotalPlaytime());
            log.info("  - 채팅 수: {}", agg.getTotalChatCount());
            log.info("  - 좋아요 수: {}", agg.getTotalLikeCount());
            log.info("  - 상품 클릭 수: {}", agg.getTotalProductClickCount());
            log.info("  - 상품 주문 수: {}", agg.getTotalProductOrderCount());
            log.info("  - 상품 주문 금액: {}", agg.getTotalProductOrderAmount());
        }
    }

    @Test
    @DisplayName("Partner ID별 집계 배치 작업 테스트")
    void partnerAggregation_배치_작업_테스트() throws Exception {
        // given
        long beforeCount = partnerAggregationService.getTotalAggregationCount();

        // when
        JobParameters jobParameters = new JobParametersBuilder()
                .addLong("time", System.currentTimeMillis())
                .toJobParameters();

        JobExecution jobExecution = jobLauncherTestUtils.launchJob(jobParameters);

        // then
        assertEquals(BatchStatus.COMPLETED, jobExecution.getStatus());

        long afterCount = partnerAggregationService.getTotalAggregationCount();
        assertTrue(afterCount > beforeCount, "집계 데이터가 생성되어야 합니다.");

        log.info("배치 작업 완료: 기존 {} 건 -> 현재 {} 건", beforeCount, afterCount);
    }

    @Test
    @DisplayName("Partner ID별 집계 성능 테스트")
    void partnerAggregation_성능_테스트() {
        // given
        LocalDateTime aggregationDate = LocalDateTime.now();

        // when
        long startTime = System.currentTimeMillis();
        partnerAggregationService.aggregateByPartnerId(aggregationDate);
        long endTime = System.currentTimeMillis();

        long duration = endTime - startTime;
        List<PartnerAggregation> aggregations = partnerAggregationService.getAggregationsByDate(aggregationDate);

        // then
        log.info("집계 성능 테스트 결과:");
        log.info("  - 소요 시간: {} 초", duration / 1000.0);
        log.info("  - 처리된 파트너 수: {} 개", aggregations.size());
        log.info("  - 초당 처리 파트너 수: {} 개/초",
                aggregations.size() / (duration / 1000.0));

        // 성능 검증 (10만건 기준 30초 이내 완료)
        assertTrue(duration < 30000, "10만건 집계는 30초 이내에 완료되어야 합니다.");
    }

    @Test
    @DisplayName("Partner ID별 집계 데이터 정합성 테스트")
    void partnerAggregation_데이터_정합성_테스트() {
        // given
        LocalDateTime aggregationDate = LocalDateTime.now();

        // when
        partnerAggregationService.aggregateByPartnerId(aggregationDate);
        List<PartnerAggregation> aggregations = partnerAggregationService.getAggregationsByDate(aggregationDate);

        // then
        for (PartnerAggregation agg : aggregations) {
            // 기본 데이터 검증
            assertNotNull(agg.getPartnerId(), "partner_id는 null이 아니어야 합니다.");
            assertNotNull(agg.getAggregationDate(), "aggregation_date는 null이 아니어야 합니다.");

            // 논리적 검증
            assertTrue(agg.getTotalUv() <= agg.getTotalPv(), "UV는 PV보다 작거나 같아야 합니다.");
            assertTrue(agg.getTotalProductOrderAmount() >= 0, "주문 금액은 0 이상이어야 합니다.");
            assertTrue(agg.getTotalProductOrderQuantity() >= 0, "주문 수량은 0 이상이어야 합니다.");

            // 집계 날짜 검증
            assertEquals(aggregationDate.toLocalDate(), agg.getAggregationDate().toLocalDate(),
                    "집계 날짜가 일치해야 합니다.");
        }

        log.info("데이터 정합성 검증 완료: {} 개 파트너", aggregations.size());
    }
} 