package com.example.batch.service;

import com.example.batch.entity.PartnerAggregation;
import com.example.batch.repository.PartnerAggregationRepository;
import com.example.batch.repository.PlayerUserLogRepository;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Service
@RequiredArgsConstructor
@Slf4j
public class PartnerAggregationService {
    private final PartnerAggregationRepository partnerAggregationRepository;
    private final PlayerUserLogRepository playerUserLogRepository;
    private final JdbcTemplate jdbcTemplate;
    
    @Transactional
    public List<PartnerAggregation> aggregateByPartnerId(LocalDateTime aggregationDate) {
        List<PartnerAggregation> partnerAggregations = new ArrayList<>();
        log.info("Partner ID별 집계 시작: {}", aggregationDate);
        
        // 기존 집계 데이터 삭제
        partnerAggregationRepository.deleteByAggregationDate(aggregationDate);
        
        log.info("playerUserLogRepository : {}", playerUserLogRepository.countAllData());
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
            partnerAggregations.add(aggregation);
        }
        
        long count = partnerAggregationRepository.countAllData();
        log.info("Partner ID별 집계 완료: {} 개 파트너", count);

        return partnerAggregations;
    }
    
    @Transactional
    public List<PartnerAggregation> aggregateByPartnerIdAndDateRange(String partnerId, LocalDateTime startDate, LocalDateTime endDate) {
        log.info("Partner ID별 집계(기간 조건) 시작: partnerId={}, startDate={}, endDate={}", partnerId, startDate, endDate);

        // 기존 집계 데이터 삭제 (기간 내 해당 partnerId)
        partnerAggregationRepository.deleteByPartnerIdAndAggregationDateBetween(partnerId, startDate, endDate);

        // SQL 쿼리를 통한 집계 수행
        String aggregationSql = """
                SELECT 
                    partner_id,
                    COUNT(DISTINCT member_id) AS total_uv,
                    COUNT(CASE WHEN action = 'pageView' THEN 1 END) AS total_pv,
                    COALESCE(SUM(playtime), 0) AS total_playtime,
                    COUNT(CASE WHEN action = 'chat' THEN 1 END) AS total_chat_count,
                    COALESCE(SUM(like_count), 0) AS total_like_count,
                    COUNT(CASE WHEN action = 'productClick' THEN 1 END) AS total_product_click_count,
                    COUNT(CASE WHEN action = 'productOrder' THEN 1 END) AS total_product_order_count,
                    COALESCE(SUM(product_order_amount), 0) AS total_product_order_amount,
                    COALESCE(SUM(product_order_quantity), 0) AS total_product_order_quantity,
                    COUNT(CASE WHEN action = 'productOrderCancel' THEN 1 END) AS total_product_order_cancel_count,
                    COUNT(CASE WHEN action = 'bannerClick' THEN 1 END) AS total_banner_click_count,
                    COUNT(CASE WHEN action = 'couponClick' THEN 1 END) AS total_coupon_click_count,
                    COUNT(CASE WHEN action = 'joinReward' THEN 1 END) AS total_reward_new_count,
                    COUNT(CASE WHEN action = 'rewardComplete' THEN 1 END) AS total_reward_complete_count,
                    COUNT(CASE WHEN action = 'purchaseVerifying' THEN 1 END) AS total_purchase_verifying_count,
                    COUNT(CASE WHEN action = 'joinQuiz' THEN 1 END) AS total_quiz_new_count,
                    COUNT(CASE WHEN action = 'shareClick' THEN 1 END) AS total_share_click_count
                FROM player_userlog 
                WHERE partner_id = ? 
                  AND __time >= ? 
                  AND __time < ? 
                GROUP BY partner_id
            """;

        List<Map<String, Object>> results = jdbcTemplate.queryForList(aggregationSql, partnerId, startDate, endDate);

        List<PartnerAggregation> result = new ArrayList<>();
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
                    .aggregationDate(startDate) // 집계 기준일을 startDate로 저장 (필요에 따라 endDate 등 조정)
                    .build();

            partnerAggregationRepository.save(aggregation);
            result.add(aggregation);
        }

        long count = partnerAggregationRepository.countAllData();
        log.info("Partner ID별 집계(기간 조건) 완료: {} 개 파트너", count);

        return result;
    }

    @Transactional
    public List<PartnerAggregation> aggregateByDateRange(LocalDateTime startDate, LocalDateTime endDate) {
        log.info("집계(기간 조건) 시작: startDate={}, endDate={}", startDate, endDate);

        // SQL 쿼리를 통한 집계 수행
        String aggregationSql = """
                SELECT 
                    partner_id,
                    COUNT(DISTINCT member_id) AS total_uv,
                    COUNT(CASE WHEN action = 'pageView' THEN 1 END) AS total_pv,
                    COALESCE(SUM(playtime), 0) AS total_playtime,
                    COUNT(CASE WHEN action = 'chat' THEN 1 END) AS total_chat_count,
                    COALESCE(SUM(like_count), 0) AS total_like_count,
                    COUNT(CASE WHEN action = 'productClick' THEN 1 END) AS total_product_click_count,
                    COUNT(CASE WHEN action = 'productOrder' THEN 1 END) AS total_product_order_count,
                    COALESCE(SUM(product_order_amount), 0) AS total_product_order_amount,
                    COALESCE(SUM(product_order_quantity), 0) AS total_product_order_quantity,
                    COUNT(CASE WHEN action = 'productOrderCancel' THEN 1 END) AS total_product_order_cancel_count,
                    COUNT(CASE WHEN action = 'bannerClick' THEN 1 END) AS total_banner_click_count,
                    COUNT(CASE WHEN action = 'couponClick' THEN 1 END) AS total_coupon_click_count,
                    COUNT(CASE WHEN action = 'joinReward' THEN 1 END) AS total_reward_new_count,
                    COUNT(CASE WHEN action = 'rewardComplete' THEN 1 END) AS total_reward_complete_count,
                    COUNT(CASE WHEN action = 'purchaseVerifying' THEN 1 END) AS total_purchase_verifying_count,
                    COUNT(CASE WHEN action = 'joinQuiz' THEN 1 END) AS total_quiz_new_count,
                    COUNT(CASE WHEN action = 'shareClick' THEN 1 END) AS total_share_click_count
                FROM player_userlog 
                WHERE __time >= ? 
                  AND __time < ? 
                GROUP BY partner_id
            """;

        List<Map<String, Object>> results = jdbcTemplate.queryForList(aggregationSql, startDate, endDate);

        List<PartnerAggregation> result = new ArrayList<>();
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
                    .aggregationDate(startDate) // 집계 기준일을 startDate로 저장 (필요에 따라 endDate 등 조정)
                    .build();

            result.add(aggregation);
        }

        return result;
    }

    public int saveBatchData(List<PartnerAggregation> aggregations) {
        log.info("Batch data save started: {} items", aggregations.size());
        int savedCount = 0;
        for (PartnerAggregation aggregation : aggregations) {
            PartnerAggregation savedAggregation = partnerAggregationRepository.save(aggregation);
            if (savedAggregation != null) {
                savedCount++;
            }
        }
        log.info("Batch data save completed: {} items saved", savedCount);
        return savedCount;
    }

    
    public List<PartnerAggregation> getAggregationsByDate(LocalDateTime date) {
        return partnerAggregationRepository.findByAggregationDate(date);
    }
    
    public long getTotalAggregationCount() {
        return partnerAggregationRepository.countAllData();
    }

    public enum AggregationType {
        DAILY, HOURLY, MINUTELY
    }

    @Transactional
    public void aggregateByPartnerAndPeriod(
            String partnerId,
            LocalDateTime startDate,
            LocalDateTime endDate,
            AggregationType aggregationType
    ) {
        String groupByFormat;
        switch (aggregationType) {
            case DAILY:
                groupByFormat = "%Y-%m-%d";
                break;
            case HOURLY:
                groupByFormat = "%Y-%m-%d %H";
                break;
            case MINUTELY:
                groupByFormat = "%Y-%m-%d %H:%i";
                break;
            default:
                throw new IllegalArgumentException("Unknown aggregation type");
        }

        String aggregationSql = String.format(
            "SELECT " +
            "  partner_id, " +
            "  DATE_FORMAT(created_at, '%s') AS aggregation_key, " +
            "  COUNT(DISTINCT member_id) AS total_uv, " +
            "  COUNT(*) AS total_pv, " +
            "  COALESCE(SUM(playtime), 0) AS total_playtime, " +
            "  SUM(CASE WHEN action = 'chat' THEN 1 ELSE 0 END) AS total_chat_count, " +
            "  COALESCE(SUM(like_count), 0) AS total_like_count, " +
            "  SUM(CASE WHEN action = 'productClick' THEN 1 ELSE 0 END) AS total_product_click_count, " +
            "  SUM(CASE WHEN action = 'productOrder' THEN 1 ELSE 0 END) AS total_product_order_count, " +
            "  COALESCE(SUM(product_order_amount), 0) AS total_product_order_amount, " +
            "  COALESCE(SUM(product_order_quantity), 0) AS total_product_order_quantity, " +
            "  SUM(CASE WHEN action = 'productOrderCancel' THEN 1 ELSE 0 END) AS total_product_order_cancel_count, " +
            "  SUM(CASE WHEN action = 'bannerClick' THEN 1 ELSE 0 END) AS total_banner_click_count, " +
            "  SUM(CASE WHEN action = 'couponClick' THEN 1 ELSE 0 END) AS total_coupon_click_count, " +
            "  SUM(CASE WHEN action = 'joinReward' THEN 1 ELSE 0 END) AS total_reward_new_count, " +
            "  SUM(CASE WHEN action = 'rewardComplete' THEN 1 ELSE 0 END) AS total_reward_complete_count, " +
            "  SUM(CASE WHEN action = 'purchaseVerifying' THEN 1 ELSE 0 END) AS total_purchase_verifying_count, " +
            "  SUM(CASE WHEN action = 'joinQuiz' THEN 1 ELSE 0 END) AS total_quiz_new_count, " +
            "  SUM(CASE WHEN action = 'shareClick' THEN 1 ELSE 0 END) AS total_share_click_count " +
            "FROM player_userlog " +
            "WHERE partner_id = ? AND created_at >= ? AND created_at < ? " +
            "GROUP BY partner_id, aggregation_key",
            groupByFormat
        );

        List<Map<String, Object>> results = jdbcTemplate.queryForList(
            aggregationSql, partnerId, startDate, endDate
        );

        for (Map<String, Object> row : results) {
            PartnerAggregation aggregation = PartnerAggregation.builder()
                    .partnerId((String) row.get("partner_id"))
                    .aggregationType(aggregationType.name())
                    .aggregationKey((String) row.get("aggregation_key"))
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
                    .aggregationDate(startDate) // 또는 aggregationKey 파싱해서 날짜로 저장
                    .build();

            partnerAggregationRepository.save(aggregation);
        }
    }
} 