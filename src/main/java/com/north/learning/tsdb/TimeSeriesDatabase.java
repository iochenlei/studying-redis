package com.north.learning.tsdb;

import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.connection.ReturnType;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public class TimeSeriesDatabase {
    private final RedisConnection redisConnection;
    private final String dbName;
    private final String hashTableName;
    private final String sortedSetName;

    public TimeSeriesDatabase(String dbName, RedisConnection redisConnection) {
        this.dbName = dbName;
        hashTableName = dbName + ":tsb";
        sortedSetName = hashTableName + ":rank";
        this.redisConnection = redisConnection;
    }

    /**
     * Inserts a record into the database.
     *
     * @param id record id
     * @param timestampMillis current UTC timestamp in millis.
     * @param data the data to be saved to the database.
     */
    public boolean insert(String id, long timestampMillis, byte[] data) {
        String luaScript = "local hashTableName = KEYS[1] .. ':tsb'\n" +
                "local recordId = ARGV[1]\n" +
                "local timestampMillis = ARGV[2]\n" +
                "local data = ARGV[3]\n" +
                "redis.call('hset', hashTableName, recordId .. ':data', data)\n" +
                "redis.call('hset', hashTableName, recordId .. ':timestamp', timestampMillis)\n" +
                "redis.call('zadd', hashTableName .. ':rank', timestampMillis, recordId)\n" +
                "return true\n";
        Boolean ret = redisConnection.scriptingCommands()
                .eval(luaScript.getBytes(), ReturnType.BOOLEAN, 1, dbName.getBytes(), id.getBytes(), Long.toString(timestampMillis).getBytes(), data);
        return ret == null ? false : ret;
    }

    /**
     * Queries all data by a range.
     *
     * @param start the start value of the range, and its minimal value is zero.
     * @param end the end value of the range(inclusive).
     * @return the result is a List of {@link Record}.
     */
    public List<Record> queryByRange(int start, int end) {
        Object rawIds = redisConnection.execute("zrange", sortedSetName.getBytes(), Integer.toString(start).getBytes(), Integer.toString(end).getBytes());
        if (rawIds instanceof List) {
            List<String> strIds = ((List<byte[]>) rawIds)
                    .stream()
                    .map(value -> new String(value))
                    .collect(Collectors.toList());
            return strIds.stream()
                    .map(this::queryById)
                    .collect(Collectors.toList());
        }
        return Collections.emptyList();
    }

    public Record queryById(String recordId) {
        String dataKey = recordId + ":data";
        String timestampMillisKey = recordId + ":timestamp";
        List<byte[]> rawRecord = (List<byte[]>)redisConnection.execute("hmget", hashTableName.getBytes(), timestampMillisKey.getBytes(), dataKey.getBytes());
        return new Record(recordId, Integer.parseInt(new String(rawRecord.get(0))), rawRecord.get(1));
    }

    private Record queryMinRecord(boolean isReverse) {
        List<byte[]> results = (List<byte[]>) redisConnection.execute(!isReverse ? "zrange" : "zrevrange", sortedSetName.getBytes(), "0".getBytes(), "0".getBytes());
        if (results.size() == 0) {
            return null;
        }
        return queryById(new String(results.get(0)));
    }

    /**
     * Queries the maximal record in the database.
     * @return it will the right record if the database is not empty. Otherwise, it will return null.
     */
    public Record queryMax() {
        return queryMinRecord(true);
    }

    /**
     * Queries the minimal record in the database.
     * @return it will the right record if the database is not empty. Otherwise, it will return null.
     */
    public Record queryMin() {
        return queryMinRecord(false);
    }

    public boolean deleteOne(String recordId) {
        String luaScript = "local hashTableName = KEYS[1] .. ':tsb'\n" +
                "local sortedSetName = hashTableName .. ':rank'\n" +
                "local recordId = ARGV[1]\n" +
                "local dataKey = recordId .. ':data'\n" +
                "local timestampMillisKey = recordId .. ':timestamp'\n" +
                "redis.call('zrem', sortedSetName, recordId)\n" +
                "redis.call('hdel', hashTableName, dataKey, timestampMillisKey)\n" +
                "return true\n" +
                "\n";
        Boolean ret = redisConnection.scriptingCommands()
                .eval(luaScript.getBytes(), ReturnType.BOOLEAN, 1, dbName.getBytes(), recordId.getBytes());
        return ret == null ? false : ret;
    }
}
