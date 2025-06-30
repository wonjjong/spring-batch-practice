package com.example.batch.reader;

import com.example.batch.entity.PlayerUserLog;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.database.JdbcCursorItemReader;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Component;

import javax.sql.DataSource;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDateTime;

@Component
@RequiredArgsConstructor
@Slf4j
public class PlayerUserLogJdbcItemReader {

    private final DataSource dataSource;

    public JdbcCursorItemReader<PlayerUserLog> createReader() {
        JdbcCursorItemReader<PlayerUserLog> reader = new JdbcCursorItemReader<>();
        
        reader.setDataSource(dataSource);
        reader.setSql("SELECT id, __time, partner_id, broadcast_state_code, broadcast_id, " +
                     "member_id, action, playtime, like_count, product_order_amount, " +
                     "product_order_quantity, broadcast_type_code, created_at " +
                     "FROM player_userlog " +
                     "ORDER BY id");
        
        reader.setRowMapper(new PlayerUserLogRowMapper());
        reader.setFetchSize(1000); // 페치 크기 설정
        reader.setMaxRows(0); // 최대 행 수 제한 없음
        
        return reader;
    }

    private static class PlayerUserLogRowMapper implements RowMapper<PlayerUserLog> {
        @Override
        public PlayerUserLog mapRow(ResultSet rs, int rowNum) throws SQLException {
            return PlayerUserLog.builder()
                    .id(rs.getLong("id"))
                    .time(rs.getObject("__time", LocalDateTime.class))
                    .partnerId(rs.getString("partner_id"))
                    .broadcastStateCode(rs.getString("broadcast_state_code"))
                    .broadcastId(rs.getString("broadcast_id"))
                    .memberId(rs.getString("member_id"))
                    .action(rs.getString("action"))
                    .playtime(rs.getObject("playtime", Integer.class))
                    .likeCount(rs.getObject("like_count", Integer.class))
                    .productOrderAmount(rs.getObject("product_order_amount", Integer.class))
                    .productOrderQuantity(rs.getObject("product_order_quantity", Integer.class))
                    .broadcastTypeCode(rs.getString("broadcast_type_code"))
                    .createdAt(rs.getObject("created_at", LocalDateTime.class))
                    .build();
        }
    }
} 