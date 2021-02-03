package com.lpq.stream.source.sensor;

/**
 * @author liupengqiang
 * @date 2021/2/3
 */
public class ThresholdUpdate {
    private String id;
    private Double threshold;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public Double getThreshold() {
        return threshold;
    }

    public void setThreshold(Double threshold) {
        this.threshold = threshold;
    }

    public ThresholdUpdate() {
    }

    public ThresholdUpdate(String id, Double threshold) {
        this.id = id;
        this.threshold = threshold;
    }

    @Override
    public String toString() {
        return "ThresholdUpdate{" +
                "id='" + id + '\'' +
                ", threshold=" + threshold +
                '}';
    }
}
