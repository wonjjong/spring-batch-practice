package com.example.batch;

import com.example.batch.entity.PlayerUserLog;
import com.example.batch.repository.PlayerUserLogRepository;
import com.example.batch.service.TestDataGeneratorService;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest
@ActiveProfiles("test")
@Slf4j
class TestDataGeneratorServiceTest {

    @Autowired
    private TestDataGeneratorService testDataGeneratorService;

    @Autowired
    private PlayerUserLogRepository playerUserLogRepository;

    @BeforeEach
    void setUp() {
        // 각 테스트 전에 데이터 초기화
        playerUserLogRepository.deleteAllData();
    }

    @Test
    @Transactional
    void testGenerateSmallBatch() {
        // 작은 배치 테스트 (100건)
        int testCount = 100;
        
        testDataGeneratorService.generateTestData(testCount);
        
        long actualCount = testDataGeneratorService.getCurrentDataCount();
        assertEquals(testCount, actualCount, "생성된 데이터 수가 정확해야 합니다.");
        
        // 데이터 품질 검증
        List<PlayerUserLog> logs = playerUserLogRepository.findAll();
        assertFalse(logs.isEmpty(), "생성된 로그가 비어있지 않아야 합니다.");
        
        PlayerUserLog firstLog = logs.get(0);
        assertNotNull(firstLog.getTime(), "시간이 설정되어야 합니다.");
        assertNotNull(firstLog.getPartnerId(), "파트너 ID가 설정되어야 합니다.");
        assertNotNull(firstLog.getBroadcastId(), "방송 ID가 설정되어야 합니다.");
        assertNotNull(firstLog.getMemberId(), "회원 ID가 설정되어야 합니다.");
        assertNotNull(firstLog.getAction(), "액션이 설정되어야 합니다.");
        assertEquals("live", firstLog.getBroadcastTypeCode(), "방송 타입은 'live'여야 합니다.");
    }

    @Test
    @Transactional
    void testDataDistribution() {
        // 데이터 분포 테스트 (1000건)
        int testCount = 1000;
        
        testDataGeneratorService.generateTestData(testCount);
        
        List<PlayerUserLog> logs = playerUserLogRepository.findAll();
        
        // 액션 타입 분포 확인
        long pageViewCount = logs.stream()
                .filter(log -> "pageView".equals(log.getAction()))
                .count();
        
        long chatCount = logs.stream()
                .filter(log -> "chat".equals(log.getAction()))
                .count();
        
        long productOrderCount = logs.stream()
                .filter(log -> "productOrder".equals(log.getAction()))
                .count();
        
        log.info("액션 분포 - pageView: {}, chat: {}, productOrder: {}", 
                pageViewCount, chatCount, productOrderCount);
        
        // 각 액션이 최소 1건 이상 생성되었는지 확인
        assertTrue(pageViewCount > 0, "pageView 액션이 최소 1건 이상 생성되어야 합니다.");
        assertTrue(chatCount > 0, "chat 액션이 최소 1건 이상 생성되어야 합니다.");
        assertTrue(productOrderCount > 0, "productOrder 액션이 최소 1건 이상 생성되어야 합니다.");
        
        // pageView 액션에서만 playtime과 like_count가 설정되는지 확인
        List<PlayerUserLog> pageViewLogs = logs.stream()
                .filter(log -> "pageView".equals(log.getAction()))
                .toList();
        
        for (PlayerUserLog log : pageViewLogs) {
            assertNotNull(log.getPlaytime(), "pageView 액션에서 playtime이 설정되어야 합니다.");
            assertNotNull(log.getLikeCount(), "pageView 액션에서 likeCount가 설정되어야 합니다.");
        }
        
        // productOrder 액션에서만 주문 관련 필드가 설정되는지 확인
        List<PlayerUserLog> productOrderLogs = logs.stream()
                .filter(log -> "productOrder".equals(log.getAction()))
                .toList();
        
        for (PlayerUserLog log : productOrderLogs) {
            assertNotNull(log.getProductOrderAmount(), "productOrder 액션에서 productOrderAmount가 설정되어야 합니다.");
            assertNotNull(log.getProductOrderQuantity(), "productOrder 액션에서 productOrderQuantity가 설정되어야 합니다.");
        }
    }

    @Test
    @Transactional
    void testPerformanceBenchmark() {
        // 성능 벤치마크 테스트 (1만건)
        int testCount = 10_000;
        
        long startTime = System.currentTimeMillis();
        testDataGeneratorService.generateTestData(testCount);
        long endTime = System.currentTimeMillis();
        
        long duration = endTime - startTime;
        long actualCount = testDataGeneratorService.getCurrentDataCount();
        
        log.info("성능 벤치마크 - 1만건 생성: {} ms ({} 초)", duration, duration / 1000.0);
        
        assertEquals(testCount, actualCount, "생성된 데이터 수가 정확해야 합니다.");
        assertTrue(duration < 10000, "1만건 생성은 10초 이내에 완료되어야 합니다.");
        
        // 초당 처리량 계산
        double throughput = (double) testCount / (duration / 1000.0);
        log.info("처리량: {:.2f} 건/초", throughput);
        
        assertTrue(throughput > 1000, "초당 1000건 이상 처리되어야 합니다.");
    }

    @Test
    @Transactional
    void testDataConsistency() {
        // 데이터 일관성 테스트
        int testCount = 100;
        
        testDataGeneratorService.generateTestData(testCount);
        
        List<PlayerUserLog> logs = playerUserLogRepository.findAll();
        
        for (PlayerUserLog log : logs) {
            // 필수 필드 검증
            assertNotNull(log.getTime(), "시간은 null이 아니어야 합니다.");
            assertNotNull(log.getPartnerId(), "파트너 ID는 null이 아니어야 합니다.");
            assertFalse(log.getPartnerId().isEmpty(), "파트너 ID는 빈 문자열이 아니어야 합니다.");
            assertNotNull(log.getBroadcastId(), "방송 ID는 null이 아니어야 합니다.");
            assertNotNull(log.getMemberId(), "회원 ID는 null이 아니어야 합니다.");
            assertFalse(log.getMemberId().isEmpty(), "회원 ID는 빈 문자열이 아니어야 합니다.");
            assertNotNull(log.getAction(), "액션은 null이 아니어야 합니다.");
            assertEquals("live", log.getBroadcastTypeCode(), "방송 타입은 'live'여야 합니다.");
            
            // 액션별 필드 검증
            if ("pageView".equals(log.getAction())) {
                assertNotNull(log.getPlaytime(), "pageView 액션에서 playtime은 null이 아니어야 합니다.");
                assertNotNull(log.getLikeCount(), "pageView 액션에서 likeCount는 null이 아니어야 합니다.");
                assertTrue(log.getPlaytime() >= 0, "playtime은 0 이상이어야 합니다.");
                assertTrue(log.getLikeCount() >= 0, "likeCount는 0 이상이어야 합니다.");
            }
            
            if ("productOrder".equals(log.getAction())) {
                assertNotNull(log.getProductOrderAmount(), "productOrder 액션에서 productOrderAmount는 null이 아니어야 합니다.");
                assertNotNull(log.getProductOrderQuantity(), "productOrder 액션에서 productOrderQuantity는 null이 아니어야 합니다.");
                assertTrue(log.getProductOrderAmount() > 0, "productOrderAmount는 0보다 커야 합니다.");
                assertTrue(log.getProductOrderQuantity() > 0, "productOrderQuantity는 0보다 커야 합니다.");
            }
        }
    }
} 