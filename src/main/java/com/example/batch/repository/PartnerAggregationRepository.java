package com.example.batch.repository;

import com.example.batch.entity.PartnerAggregation;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.time.LocalDateTime;
import java.util.List;

@Repository
public interface PartnerAggregationRepository extends JpaRepository<PartnerAggregation, Long> {
    
    @Modifying
    @Query(value = "DELETE FROM partner_aggregation WHERE aggregation_date = :date", nativeQuery = true)
    void deleteByAggregationDate(@Param("date") LocalDateTime date);
    
    @Query(value = "SELECT * FROM partner_aggregation WHERE aggregation_date = :date", nativeQuery = true)
    List<PartnerAggregation> findByAggregationDate(@Param("date") LocalDateTime date);
    
    @Query(value = "SELECT COUNT(*) FROM partner_aggregation", nativeQuery = true)
    long countAllData();
} 