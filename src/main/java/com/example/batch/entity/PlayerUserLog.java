package com.example.batch.entity;

import jakarta.persistence.*;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.hibernate.annotations.CreationTimestamp;

import java.time.LocalDateTime;

@Entity
@Table(name = "player_userlog")
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class PlayerUserLog {
    
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;
    
    @Column(name = "__time")
    private LocalDateTime time;
    
    @Column(name = "partner_id")
    private String partnerId;
    
    @Column(name = "broadcast_state_code")
    private String broadcastStateCode;
    
    @Column(name = "broadcast_id")
    private String broadcastId;
    
    @Column(name = "member_id")
    private String memberId;
    
    @Column(name = "action")
    private String action;
    
    @Column(name = "playtime")
    private Integer playtime;
    
    @Column(name = "like_count")
    private Integer likeCount;
    
    @Column(name = "product_order_amount")
    private Integer productOrderAmount;
    
    @Column(name = "product_order_quantity")
    private Integer productOrderQuantity;
    
    @Column(name = "broadcast_type_code")
    private String broadcastTypeCode;
    
    @CreationTimestamp
    @Column(name = "created_at")
    private LocalDateTime createdAt;
} 