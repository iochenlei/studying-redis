package com.north.learning.redis;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.serializer.GenericJackson2JsonRedisSerializer;

import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertEquals;

@SpringBootTest
public class FirstDayTests {
    public static class DTO1 {
        private String v;

        public String getV() {
            return v;
        }

        public void setV(String v) {
            this.v = v;
        }
    }

    @Autowired
    private RedisTemplate redisTemplate;

    @Test
    void test2() {
        DTO1 dto = new DTO1();
        dto.setV("abc");
        redisTemplate.setValueSerializer(new GenericJackson2JsonRedisSerializer("gg"));
        redisTemplate.opsForValue().set("dto", Arrays.asList(dto));
    }

    @Test
    void test() {
        redisTemplate.opsForValue().set("mykey", "v1");
        assertEquals("v1", redisTemplate.opsForValue().get("mykey"));
    }
}
