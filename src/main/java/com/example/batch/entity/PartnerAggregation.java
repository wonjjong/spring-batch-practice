package com.example.batch.entity;

import jakarta.persistence.*;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.hibernate.annotations.CreationTimestamp;

import java.time.LocalDateTime;

@Entity
@Table(name = "partner_aggregation")
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class PartnerAggregation {
    
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;
    
    @Column(name = "partner_id")
    private String partnerId;
    
    @Column(name = "total_uv")
    private Long totalUv; // 총 유니크 방문자 수
    
    @Column(name = "total_pv")
    private Long totalPv; // 총 페이지뷰 수
    
    @Column(name = "total_playtime")
    private Long totalPlaytime; // 총 재생 시간
    
    @Column(name = "total_chat_count")
    private Long totalChatCount; // 총 채팅 수
    
    @Column(name = "total_like_count")
    private Long totalLikeCount; // 총 좋아요 수
    
    @Column(name = "total_product_click_count")
    private Long totalProductClickCount; // 총 상품 클릭 수
    
    @Column(name = "total_product_order_count")
    private Long totalProductOrderCount; // 총 상품 주문 수
    
    @Column(name = "total_product_order_amount")
    private Long totalProductOrderAmount; // 총 상품 주문 금액
    
    @Column(name = "total_product_order_quantity")
    private Long totalProductOrderQuantity; // 총 상품 주문 수량
    
    @Column(name = "total_product_order_cancel_count")
    private Long totalProductOrderCancelCount; // 총 상품 주문 취소 수
    
    @Column(name = "total_banner_click_count")
    private Long totalBannerClickCount; // 총 배너 클릭 수
    
    @Column(name = "total_coupon_click_count")
    private Long totalCouponClickCount; // 총 쿠폰 클릭 수
    
    @Column(name = "total_reward_new_count")
    private Long totalRewardNewCount; // 총 리워드 신규 참여 수
    
    @Column(name = "total_reward_complete_count")
    private Long totalRewardCompleteCount; // 총 리워드 완료 수
    
    @Column(name = "total_purchase_verifying_count")
    private Long totalPurchaseVerifyingCount; // 총 구매 인증 수
    
    @Column(name = "total_quiz_new_count")
    private Long totalQuizNewCount; // 총 퀴즈 신규 참여 수
    
    @Column(name = "total_share_click_count")
    private Long totalShareClickCount; // 총 공유 클릭 수
    
    @Column(name = "aggregation_date")
    private LocalDateTime aggregationDate; // 집계 날짜
    
    @CreationTimestamp
    @Column(name = "created_at")
    private LocalDateTime createdAt;
} 