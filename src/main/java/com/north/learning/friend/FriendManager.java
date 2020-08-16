package com.north.learning.friend;

import org.springframework.data.redis.core.RedisTemplate;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

public class FriendManager {
    private static final String FRIEND_SET_PREFIX = "FRIENDS";
    private static final String FRIEND_ALL = "ALL";

    private RedisTemplate<String, String> redisTemplate;

    public FriendManager(RedisTemplate<String, String> redisTemplate) {
        this.redisTemplate = redisTemplate;
    }

    public void removeAllPairs() {
        List<String> keys = redisTemplate.boundSetOps(getKey(FRIEND_ALL))
                .members()
                .stream()
                .map(this::getKey)
                .collect(Collectors.toList());
        redisTemplate.delete(keys);
        redisTemplate.delete(getKey(FRIEND_ALL));
    }

    private String getKey(String userId) {
        return FRIEND_SET_PREFIX + ":" + userId;
    }

    public void addPair(String userA, String userB) {
        this.redisTemplate.boundSetOps(getKey(userA))
                .add(userB);
        this.redisTemplate.boundSetOps(getKey(userB))
                .add(userA);
        this.redisTemplate.boundSetOps(getKey(FRIEND_ALL))
                .add(userA, userB);
    }

    public void removePair(String userA, String userB) {
        this.redisTemplate.boundSetOps(getKey(userA))
                .remove(userB);
        this.redisTemplate.boundSetOps(getKey(userB))
                .remove(userA);
    }

    public Collection<String> getMyFriends(String userId) {
        return this.redisTemplate.boundSetOps(getKey(userId)).members();
    }

    public Collection<String> getMutualFriends(String myselfUserId, String ...otherUserIds) {
        if (myselfUserId == null || myselfUserId.equals("")) {
            throw new IllegalArgumentException("myselfUserId must not be null or empty.");
        }
        if (otherUserIds == null) {
            throw new IllegalArgumentException("userIds must not be null.");
        }
        return this.redisTemplate.boundSetOps(getKey(myselfUserId))
                .intersect(Arrays.stream(otherUserIds).map(this::getKey).collect(Collectors.toList()));
    }

    public Collection<String> getFriendsOnlyBelongMyself(String myselfUserId, String ...otherUserIds) {
        if (myselfUserId == null || myselfUserId.equals("")) {
            throw new IllegalArgumentException("myselfUserId must not be null or empty.");
        }
        if (otherUserIds == null) {
            throw new IllegalArgumentException("userIds must not be null.");
        }
        return this.redisTemplate.boundSetOps(getKey(myselfUserId))
                .diff(Arrays.stream(otherUserIds).map(this::getKey).collect(Collectors.toList()));
    }
}
