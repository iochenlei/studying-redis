package com.north.learning.friend;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.redis.core.RedisTemplate;

import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertIterableEquals;

@SpringBootTest
class FriendManagerTest {
    @Autowired
    private RedisTemplate<String, String> redisTemplate;
    private FriendManager friendManager;

    @BeforeEach
    void setUp() {
        friendManager = new FriendManager(redisTemplate);
    }

    @AfterEach
    void tearDown() {
        friendManager.removeAllPairs();
    }

    @Test
    void testAddPair() {
        friendManager.addPair("uid1", "uid2");
        assertIterableEquals(Arrays.asList("uid2"), friendManager.getMyFriends("uid1"));
        assertIterableEquals(Arrays.asList("uid1"), friendManager.getMyFriends("uid2"));
    }

    @Test
    void testGetMyFriends() {
        friendManager.addPair("uid1", "uid2");
        friendManager.addPair("uid1", "uid3");
        assertIterableEquals(Arrays.asList("uid2", "uid3"), friendManager.getMyFriends("uid1"));
    }

    @Test
    void getGetMutualFriends() {
        friendManager.addPair("uid1", "uid2");
        friendManager.addPair("uid1", "uid3");
        friendManager.addPair("uid2", "uid3");
        friendManager.addPair("uid2", "uid4");
        friendManager.addPair("uid3", "uid5");
        assertIterableEquals(Arrays.asList("uid3"), friendManager.getMutualFriends("uid1", "uid2"));
    }

    @Test
    void getGetFriendsOnlyBelongMyself() {
        friendManager.addPair("uid1", "uid2");
        friendManager.addPair("uid1", "uid3");
        friendManager.addPair("uid1", "uid6");
        friendManager.addPair("uid2", "uid3");
        friendManager.addPair("uid2", "uid4");
        friendManager.addPair("uid3", "uid5");
        assertIterableEquals(Arrays.asList("uid6"), friendManager.getFriendsOnlyBelongMyself("uid1", "uid2", "uid3", "uid4"));
    }
}