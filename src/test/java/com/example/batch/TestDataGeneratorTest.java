package com.example.batch;

import com.example.batch.service.TestDataGeneratorService;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.transaction.annotation.Transactional;

import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest
@ActiveProfiles("test")
@Slf4j
class TestDataGeneratorTest {

    @Autowired
    private TestDataGeneratorService testDataGeneratorService;

    @Test
    @Transactional
    void generateSmallTestData() {
        // 작은 규모의 테스트 데이터 생성 (1만건)
        int testCount = 10_000;
        
        log.info("작은 규모 테스트 데이터 생성 시작: {} 건", testCount);
        
        long startTime = System.currentTimeMillis();
        testDataGeneratorService.generateJdbcTestData(testCount);
        long endTime = System.currentTimeMillis();
        
        long duration = endTime - startTime;
        long actualCount = testDataGeneratorService.getCurrentDataCount();
        
        log.info("작은 규모 테스트 데이터 생성 완료!");
        log.info("요청된 데이터 수: {} 건", testCount);
        log.info("실제 생성된 데이터 수: {} 건", actualCount);
        log.info("소요 시간: {} 초", duration / 1000.0);
        
        assertEquals(testCount, actualCount, "생성된 데이터 수가 요청한 수와 일치해야 합니다.");
    }

    @Test
    @Transactional
    void generateMediumTestData() {
        // 중간 규모의 테스트 데이터 생성 (10만건)
        int testCount = 100_000;
        
        log.info("중간 규모 테스트 데이터 생성 시작: {} 건", testCount);
        
        long startTime = System.currentTimeMillis();
        testDataGeneratorService.generateJdbcTestData(testCount);
        long endTime = System.currentTimeMillis();
        
        long duration = endTime - startTime;
        long actualCount = testDataGeneratorService.getCurrentDataCount();
        
        log.info("중간 규모 테스트 데이터 생성 완료!");
        log.info("요청된 데이터 수: {} 건", testCount);
        log.info("실제 생성된 데이터 수: {} 건", actualCount);
        log.info("소요 시간: {} 초", duration / 1000.0);
        
        assertEquals(testCount, actualCount, "생성된 데이터 수가 요청한 수와 일치해야 합니다.");
    }

    @Test
    @Transactional
    void generateLargeTestData() {
        // 대규모 테스트 데이터 생성 (100만건)
        int testCount = 1_000_000;
        
        log.info("대규모 테스트 데이터 생성 시작: {} 건", testCount);
        
        long startTime = System.currentTimeMillis();
        testDataGeneratorService.generateJdbcTestData(testCount);
        long endTime = System.currentTimeMillis();
        
        long duration = endTime - startTime;
        long actualCount = testDataGeneratorService.getCurrentDataCount();
        
        log.info("대규모 테스트 데이터 생성 완료!");
        log.info("요청된 데이터 수: {} 건", testCount);
        log.info("실제 생성된 데이터 수: {} 건", actualCount);
        log.info("소요 시간: {} 초", duration / 1000.0);
        
        assertEquals(testCount, actualCount, "생성된 데이터 수가 요청한 수와 일치해야 합니다.");
    }

    @Test
    @Transactional
    void generateFullTestData() {
        // 전체 테스트 데이터 생성 (1000만건)
        int testCount = 10_000_000;
        
        log.info("전체 테스트 데이터 생성 시작: {} 건", testCount);
        
        long startTime = System.currentTimeMillis();
        testDataGeneratorService.generateJdbcTestData(testCount);
        long endTime = System.currentTimeMillis();
        
        long duration = endTime - startTime;
        long actualCount = testDataGeneratorService.getCurrentDataCount();
        
        log.info("전체 테스트 데이터 생성 완료!");
        log.info("요청된 데이터 수: {} 건", testCount);
        log.info("실제 생성된 데이터 수: {} 건", actualCount);
        log.info("소요 시간: {} 초", duration / 1000.0);
        
        assertEquals(testCount, actualCount, "생성된 데이터 수가 요청한 수와 일치해야 합니다.");
    }

    @Test
    void testDataCount() {
        // 데이터 개수 조회 테스트
        long count = testDataGeneratorService.getCurrentDataCount();
        log.info("현재 데이터 개수: {} 건", count);
        
        assertTrue(count >= 0, "데이터 개수는 0 이상이어야 합니다.");
    }
} 