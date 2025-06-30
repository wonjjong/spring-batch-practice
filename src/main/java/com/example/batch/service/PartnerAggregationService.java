package com.example.batch.service;

import com.example.batch.entity.PartnerAggregation;
import com.example.batch.repository.PartnerAggregationRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;

@Service
@RequiredArgsConstructor
@Slf4j
public class PartnerAggregationService {
    
    private final PartnerAggregationRepository partnerAggregationRepository;
    private final JdbcTemplate jdbcTemplate;
    
    @Transactional
    public void aggregateByPartnerId(LocalDateTime aggregationDate) {
        log.info("Partner ID별 집계 시작: {}", aggregationDate);
        
        // 기존 집계 데이터 삭제
        partnerAggregationRepository.deleteByAggregationDate(aggregationDate);
        
        // SQL 쿼리를 통한 집계 수행
        String aggregationSql = """
            SELECT 
                partner_id,
                COUNT(DISTINCT member_id) AS total_uv,
                COUNT(*) FILTER (WHERE action = 'pageView') AS total_pv,
                COALESCE(SUM(playtime), 0) AS total_playtime,
                COUNT(*) FILTER (WHERE action = 'chat') AS total_chat_count,
                COALESCE(SUM(like_count), 0) AS total_like_count,
                COUNT(*) FILTER (WHERE action = 'productClick') AS total_product_click_count,
                COUNT(*) FILTER (WHERE action = 'productOrder') AS total_product_order_count,
                COALESCE(SUM(product_order_amount), 0) AS total_product_order_amount,
                COALESCE(SUM(product_order_quantity), 0) AS total_product_order_quantity,
                COUNT(*) FILTER (WHERE action = 'productOrderCancel') AS total_product_order_cancel_count,
                COUNT(*) FILTER (WHERE action = 'bannerClick') AS total_banner_click_count,
                COUNT(*) FILTER (WHERE action = 'couponClick') AS total_coupon_click_count,
                COUNT(*) FILTER (WHERE action = 'joinReward') AS total_reward_new_count,
                COUNT(*) FILTER (WHERE action = 'rewardComplete') AS total_reward_complete_count,
                COUNT(*) FILTER (WHERE action = 'purchaseVerifying') AS total_purchase_verifying_count,
                COUNT(*) FILTER (WHERE action = 'joinQuiz') AS total_quiz_new_count,
                COUNT(*) FILTER (WHERE action = 'shareClick') AS total_share_click_count
            FROM player_userlog 
            WHERE partner_id IS NOT NULL 
            GROUP BY partner_id
            """;
        
        List<Map<String, Object>> results = jdbcTemplate.queryForList(aggregationSql);
        
        for (Map<String, Object> row : results) {
            PartnerAggregation aggregation = PartnerAggregation.builder()
                    .partnerId((String) row.get("partner_id"))
                    .totalUv(((Number) row.get("total_uv")).longValue())
                    .totalPv(((Number) row.get("total_pv")).longValue())
                    .totalPlaytime(((Number) row.get("total_playtime")).longValue())
                    .totalChatCount(((Number) row.get("total_chat_count")).longValue())
                    .totalLikeCount(((Number) row.get("total_like_count")).longValue())
                    .totalProductClickCount(((Number) row.get("total_product_click_count")).longValue())
                    .totalProductOrderCount(((Number) row.get("total_product_order_count")).longValue())
                    .totalProductOrderAmount(((Number) row.get("total_product_order_amount")).longValue())
                    .totalProductOrderQuantity(((Number) row.get("total_product_order_quantity")).longValue())
                    .totalProductOrderCancelCount(((Number) row.get("total_product_order_cancel_count")).longValue())
                    .totalBannerClickCount(((Number) row.get("total_banner_click_count")).longValue())
                    .totalCouponClickCount(((Number) row.get("total_coupon_click_count")).longValue())
                    .totalRewardNewCount(((Number) row.get("total_reward_new_count")).longValue())
                    .totalRewardCompleteCount(((Number) row.get("total_reward_complete_count")).longValue())
                    .totalPurchaseVerifyingCount(((Number) row.get("total_purchase_verifying_count")).longValue())
                    .totalQuizNewCount(((Number) row.get("total_quiz_new_count")).longValue())
                    .totalShareClickCount(((Number) row.get("total_share_click_count")).longValue())
                    .aggregationDate(aggregationDate)
                    .build();
            
            partnerAggregationRepository.save(aggregation);
        }
        
        long count = partnerAggregationRepository.countAllData();
        log.info("Partner ID별 집계 완료: {} 개 파트너", count);
    }
    
    public List<PartnerAggregation> getAggregationsByDate(LocalDateTime date) {
        return partnerAggregationRepository.findByAggregationDate(date);
    }
    
    public long getTotalAggregationCount() {
        return partnerAggregationRepository.countAllData();
    }
} 