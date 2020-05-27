package com.lpq.stream.model;

/**
 * @author liupengqiang
 * @date 2020/5/19
 */
public class MinMaxTemp {
    private String id;
    private double min;
    private double max;
    private long endTs;

    public MinMaxTemp() {
    }

    public MinMaxTemp(String id, double min, double max, long endTs) {
        this.id = id;
        this.min = min;
        this.max = max;
        this.endTs = endTs;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public double getMin() {
        return min;
    }

    public void setMin(double min) {
        this.min = min;
    }

    public double getMax() {
        return max;
    }

    public void setMax(double max) {
        this.max = max;
    }

    public long getEndTs() {
        return endTs;
    }

    public void setEndTs(long endTs) {
        this.endTs = endTs;
    }

    @Override
    public String toString() {
        return "MinMaxTemp{" +
                "id='" + id + '\'' +
                ", min=" + min +
                ", max=" + max +
                ", endTs=" + endTs +
                '}';
    }
}
