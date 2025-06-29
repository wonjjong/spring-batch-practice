package com.example.batch.repository;

import com.example.batch.entity.PlayerUserLog;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.stereotype.Repository;

@Repository
public interface PlayerUserLogRepository extends JpaRepository<PlayerUserLog, Long> {
    
    @Modifying
    @Query(value = "DELETE FROM player_userlog", nativeQuery = true)
    void deleteAllData();
    
    @Query(value = "SELECT COUNT(*) FROM player_userlog", nativeQuery = true)
    long countAllData();
} 