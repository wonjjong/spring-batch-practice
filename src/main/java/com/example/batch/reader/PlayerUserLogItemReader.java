package com.example.batch.reader;

import com.example.batch.entity.PlayerUserLog;
import com.example.batch.repository.PlayerUserLogRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.NonTransientResourceException;
import org.springframework.batch.item.ParseException;
import org.springframework.batch.item.UnexpectedInputException;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Component;

import java.util.Iterator;

@Component
@RequiredArgsConstructor
@Slf4j
public class PlayerUserLogItemReader implements ItemReader<PlayerUserLog> {

    private final PlayerUserLogRepository playerUserLogRepository;
    
    private Iterator<PlayerUserLog> currentPageIterator;
    private int currentPage = 0;
    private static final int PAGE_SIZE = 1000; // 페이지 크기
    private boolean isInitialized = false;

    @Override
    public PlayerUserLog read() throws Exception, UnexpectedInputException, ParseException, NonTransientResourceException {
        if (!isInitialized) {
            initializeReader();
        }

        if (currentPageIterator != null && currentPageIterator.hasNext()) {
            return currentPageIterator.next();
        }

        // 현재 페이지의 데이터를 모두 읽었으면 다음 페이지 로드
        Page<PlayerUserLog> nextPage = loadNextPage();
        if (nextPage != null && nextPage.hasContent()) {
            currentPageIterator = nextPage.iterator();
            return currentPageIterator.next();
        }

        // 더 이상 읽을 데이터가 없음
        return null;
    }

    private void initializeReader() {
        log.info("PlayerUserLog ItemReader 초기화 시작");
        currentPage = 0;
        currentPageIterator = null;
        isInitialized = true;
        log.info("PlayerUserLog ItemReader 초기화 완료");
    }

    private Page<PlayerUserLog> loadNextPage() {
        try {
            Pageable pageable = PageRequest.of(currentPage, PAGE_SIZE);
            Page<PlayerUserLog> page = playerUserLogRepository.findAll(pageable);
            
            if (page.hasContent()) {
                log.debug("페이지 {} 로드 완료: {} 건", currentPage, page.getNumberOfElements());
                currentPage++;
                return page;
            } else {
                log.info("모든 페이지 로드 완료. 총 {} 페이지 처리", currentPage);
                return null;
            }
        } catch (Exception e) {
            log.error("페이지 {} 로드 중 오류 발생", currentPage, e);
            throw new RuntimeException("데이터 로드 중 오류 발생", e);
        }
    }

    public void reset() {
        isInitialized = false;
        currentPage = 0;
        currentPageIterator = null;
    }
} 