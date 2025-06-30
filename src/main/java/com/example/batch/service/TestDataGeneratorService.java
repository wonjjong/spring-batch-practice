package com.example.batch.service;

import com.example.batch.entity.PlayerUserLog;
import com.example.batch.repository.PlayerUserLogRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.BatchPreparedStatementSetter;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

@Service
@RequiredArgsConstructor
@Slf4j
public class TestDataGeneratorService {
    
    private final PlayerUserLogRepository playerUserLogRepository;
    private final JdbcTemplate jdbcTemplate;
    private static final String[] PARTNER_IDS = {"partner_001", "partner_002", "partner_003", "partner_004", "partner_005"};
    private static final String[] BROADCAST_STATE_CODES = {"cancel", "standby", "accepted", "ready", "onair", "warning", "error", "converting"};
    private static final String[] ACTIONS = {"pageView", "chat", "productClick", "productOrder", "productOrderCancel", 
                                           "bannerClick", "couponClick", "joinReward", "rewardComplete", 
                                           "purchaseVerifying", "joinQuiz", "shareClick"};
    
    @Transactional
    public void generateJpaTestData(int totalCount) {
        log.info("테스트 데이터 생성 시작: {} 건", totalCount);
        
        // 기존 데이터 삭제
        playerUserLogRepository.deleteAllData();
        log.info("기존 데이터 삭제 완료");
        
        int batchSize = 10000; // 배치 크기
        int totalBatches = (int) Math.ceil((double) totalCount / batchSize);
        
        Random random = new Random();
        LocalDateTime startTime = LocalDateTime.now().minusDays(30); // 30일 전부터
        
        for (int batch = 0; batch < totalBatches; batch++) {
            List<PlayerUserLog> batchData = new ArrayList<>();
            
            int currentBatchSize = Math.min(batchSize, totalCount - (batch * batchSize));
            
            for (int i = 0; i < currentBatchSize; i++) {
                PlayerUserLog log = createRandomLog(random, startTime);
                batchData.add(log);
            }
            
            playerUserLogRepository.saveAll(batchData);
            
            if (batch % 10 == 0) {
                log.info("진행률: {}/{} 배치 완료 ({}%)", 
                    batch + 1, totalBatches, 
                    Math.round((double) (batch + 1) / totalBatches * 100));
            }
        }
        
        long actualCount = playerUserLogRepository.countAllData();
        log.info("테스트 데이터 생성 완료: {} 건", actualCount);
    }

    public void generateJdbcTestData(int totalCount) {
        log.info("테스트 데이터 생성 시작: {} 건", totalCount);

        jdbcTemplate.update("DELETE FROM player_userlog");
        log.info("기존 데이터 삭제 완료");

        int batchSize = 10000;
        int totalBatches = (int) Math.ceil((double) totalCount / batchSize);

        Random random = new Random();
        LocalDateTime startTime = LocalDateTime.now().minusDays(30);

        for (int batch = 0; batch < totalBatches; batch++) {
            List<PlayerUserLog> batchData = new ArrayList<>();

            int currentBatchSize = Math.min(batchSize, totalCount - (batch * batchSize));

            for (int i = 0; i < currentBatchSize; i++) {
                batchData.add(createRandomLog(random, startTime));
            }

            batchInsert(batchData);

            if (batch % 10 == 0) {
                log.info("진행률: {}/{} 배치 완료 ({}%)",
                        batch + 1, totalBatches,
                        Math.round((double) (batch + 1) / totalBatches * 100));
            }
        }

        Long count = jdbcTemplate.queryForObject("SELECT COUNT(*) FROM player_userlog", Long.class);
        log.info("테스트 데이터 생성 완료: {} 건", count);
    }


    private void batchInsert(List<PlayerUserLog> dataList) {
        String sql = "INSERT INTO player_userlog " +
                "(__time, partner_id, broadcast_state_code, broadcast_id, member_id, action, " +
                "playtime, like_count, product_order_amount, product_order_quantity, broadcast_type_code) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

        jdbcTemplate.batchUpdate(sql, new BatchPreparedStatementSetter() {
            @Override
            public void setValues(PreparedStatement ps, int i) throws SQLException {
                PlayerUserLog log = dataList.get(i);
                ps.setObject(1, log.getTime());
                ps.setString(2, log.getPartnerId());
                ps.setString(3, log.getBroadcastStateCode());
                ps.setString(4, log.getBroadcastId());
                ps.setString(5, log.getMemberId());
                ps.setString(6, log.getAction());
                if (log.getPlaytime() != null) ps.setInt(7, log.getPlaytime()); else ps.setNull(7, java.sql.Types.INTEGER);
                if (log.getLikeCount() != null) ps.setInt(8, log.getLikeCount()); else ps.setNull(8, java.sql.Types.INTEGER);
                if (log.getProductOrderAmount() != null) ps.setInt(9, log.getProductOrderAmount()); else ps.setNull(9, java.sql.Types.INTEGER);
                if (log.getProductOrderQuantity() != null) ps.setInt(10, log.getProductOrderQuantity()); else ps.setNull(10, java.sql.Types.INTEGER);
                ps.setString(11, log.getBroadcastTypeCode());
            }

            @Override
            public int getBatchSize() {
                return dataList.size();
            }
        });
    }


    private PlayerUserLog createRandomLog(Random random, LocalDateTime startTime) {
        String action = ACTIONS[random.nextInt(ACTIONS.length)];
        
        return PlayerUserLog.builder()
                .time(startTime.plusSeconds(random.nextInt(30 * 24 * 60 * 60))) // 30일 내 랜덤 시간
                .partnerId(PARTNER_IDS[random.nextInt(PARTNER_IDS.length)])
                .broadcastStateCode(BROADCAST_STATE_CODES[random.nextInt(BROADCAST_STATE_CODES.length)])
                .broadcastId("broadcast_" + String.format("%06d", random.nextInt(1000000)))
                .memberId("member_" + String.format("%08d", random.nextInt(100000000)))
                .action(action)
                .playtime(action.equals("pageView") ? random.nextInt(3600) : null) // pageView일 때만 재생시간
                .likeCount(action.equals("pageView") ? random.nextInt(10) : null) // pageView일 때만 좋아요
                .productOrderAmount(action.equals("productOrder") ? random.nextInt(100000) + 1000 : null) // 주문액
                .productOrderQuantity(action.equals("productOrder") ? random.nextInt(5) + 1 : null) // 주문수량
                .broadcastTypeCode("live")
                .build();
    }
    
    public long getCurrentDataCount() {
        return playerUserLogRepository.countAllData();
    }
} 