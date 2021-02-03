package com.lpq.connector.dao;

/**
 * @author liupengqiang
 * @date 2020/6/10
 */
public class ConsoleRecord {
    private long timestamp;
    private int id;
    private String value;

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    public ConsoleRecord() {
    }

    public ConsoleRecord(long timestamp, int id, String value) {
        this.timestamp = timestamp;
        this.id = id;
        this.value = value;
    }

    @Override
    public String toString() {
        return "ConsoleRecord{" +
                "timestamp=" + timestamp +
                ", id=" + id +
                ", value='" + value + '\'' +
                '}';
    }
}
