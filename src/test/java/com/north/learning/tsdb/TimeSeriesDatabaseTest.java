package com.north.learning.tsdb;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

@SpringBootTest(classes = TimeSeriesDatabaseTest.TestConfig.class)
class TimeSeriesDatabaseTest {
    @Autowired
    RedisConnectionFactory redisConnectionFactory;
    private TimeSeriesDatabase db;

    @BeforeEach
    void setUp() {
        RedisConnection connection = redisConnectionFactory.getConnection();
        db = new TimeSeriesDatabase("test", connection);
    }

    @Configuration
    public static class TestConfig {
        @Bean
        public RedisConnectionFactory redisConnectionFactory() {
            return new LettuceConnectionFactory();
        }
    }

    @Test
    void testInsert() {
        db.insert("123", 1, "abc".getBytes());
        try {
            Record record = db.queryByRange(0, 0).get(0);
            assertEquals("123", record.getId());
            assertEquals(1, record.getTimestampMillis());
            assertArrayEquals("abc".getBytes(), record.getData());
        } finally {
            db.deleteOne("123");
        }
    }

    @Test
    void testQueryByRange() {
        db.insert("1", 1, "a".getBytes());
        db.insert("2", 2, "b".getBytes());
        db.insert("3", 3, "c".getBytes());
        db.insert("4", -1, "d".getBytes());

        List<Record> records = db.queryByRange(1, 2);
        assertEquals("1", records.get(0).getId());
        assertEquals("2", records.get(1).getId());

        // who is the first item?
        assertEquals("4", db.queryByRange(0, 0).get(0).getId());

        db.deleteOne("1");
        db.deleteOne("2");
        db.deleteOne("3");
        db.deleteOne("4");
    }

    @Test
    void testQueryMin() {
        db.insert("1", 1, "a".getBytes());
        db.insert("2", 2, "a".getBytes());

        Record maxRecord = db.queryMax();
        Record minRecord = db.queryMin();
        assertEquals("2", maxRecord.getId());
        assertEquals("1", minRecord.getId());

        db.deleteOne("1");
        db.deleteOne("2");
    }
}