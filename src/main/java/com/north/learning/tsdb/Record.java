package com.north.learning.tsdb;

public final class Record {
    private String id;
    private long timestampMillis;
    private byte[] data;

    public Record(String id, long timestampMillis, byte[] data) {
        this.id = id;
        this.timestampMillis = timestampMillis;
        this.data = data;
    }

    public String getId() {
        return id;
    }

    public long getTimestampMillis() {
        return timestampMillis;
    }

    public byte[] getData() {
        return data;
    }
}
