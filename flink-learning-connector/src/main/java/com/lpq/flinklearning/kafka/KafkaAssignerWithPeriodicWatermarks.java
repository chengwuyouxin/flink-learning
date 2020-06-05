package com.lpq.flinklearning.kafka;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;

import javax.annotation.Nullable;

/**
 * @author liupengqiang
 * @date 2020/5/25
 *
 * FlinkKafkaConsumer011将发出带有时间戳的记录，但是不会发出watermark,需要自己配置watermark生成类
 */
public class KafkaAssignerWithPeriodicWatermarks implements AssignerWithPeriodicWatermarks<ObjectNode> {

    Long bound = 10 * 1000L;
    Long maxTs = Long.MIN_VALUE;

    @Nullable
    @Override
    public Watermark getCurrentWatermark() {
        return new Watermark(maxTs - bound);
    }

    //使用 Kafka 的时间戳时，无需定义时间戳提取器。
    // extractTimestamp() 方法的 previousElementTimestamp 参数包含 Kafka 消息携带的时间戳。
    @Override
    public long extractTimestamp(ObjectNode jsonNodes, long lastTimestamp) {
        maxTs = Math.max(maxTs,lastTimestamp);
        return lastTimestamp;
    }
}
